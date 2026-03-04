import asyncio
import dataclasses
import enum
import logging
from pathlib import Path
from typing import Optional, TYPE_CHECKING

from globus_sdk import TransferData, GlobusHTTPResponse

if TYPE_CHECKING:
    from globus_sdk import TransferClient

from esgpull import File
from esgpull.downloader.base import (
    DownloadTask,
    FileResult,
    TaskHeartbeat,
    TaskResult,
    TaskStartInfo,
)
from esgpull.models.file import FileStatus


@dataclasses.dataclass
class GlobusTaskStartInfo(TaskStartInfo):
    globus_task_id: str


@dataclasses.dataclass
class GlobusTaskHeartbeat(TaskHeartbeat):
    globus_task_id: str = dataclasses.field(kw_only=True)


@dataclasses.dataclass
class GlobusTaskResult(TaskResult):
    globus_task_id: str = dataclasses.field(kw_only=True)


class _GlobusTransferStatus(enum.Enum):
    """
    Ref: https://docs.globus.org/api/transfer/task/#task_document
    """
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class GlobusDownloadTask(DownloadTask):
    """
    Transfer a batch of files to a globus endpoint.
    """
    def __init__(
            self,
            task_label,
            files,
            client: TransferClient,

            # All files in this batch must be in same source collection
            source_collection_id: str,

            # Typically set in esgpull config
            dest_collection_id: str,
            dest_root_path: str,

            # This is used to check an existing task in progress
            transfer_id: Optional[str] = None,

            poll_time: int = 60 * 1,
            wait_until_resolved: bool = True,
    ) -> None:
        super().__init__(task_label, files)

        self._client = client
        self._source_collection_id = source_collection_id

        self._dest_collection_id = dest_collection_id
        self._dest_root_path = dest_root_path

        # For long transfers, default poll time will increase up to a system-provided max
        self._poll_time = poll_time
        self._wait_until_resolved = wait_until_resolved
        self._transfer_id = transfer_id

    def to_fail(self, msg: str = "An unknown error occurred") -> GlobusTaskResult:
        fr = FileResult.fail_all(self._files)
        return GlobusTaskResult(
            self._task_label,
            fr,
            msg,
            self._start_time,
            globus_task_id=self._transfer_id,
        )


    ########## Internal helpers
    def _emit_heartbeat(self, n_files_completed: int, bytes_completed: int) -> None:
        event = GlobusTaskHeartbeat(
            self._task_label,
            n_files_completed,
            self._files_expected,
            bytes_completed,
            self._bytes_expected,
            globus_task_id=self._transfer_id,
        )
        for callback in self._heartbeat_callbacks:
            callback(event)

    def _make_transfer_data(self, files: list[File]) -> TransferData:
        td = TransferData(
            self._source_collection_id,
            self._dest_collection_id,
            skip_source_errors=True,  # if a file doesn't exist, we'll capture from event ,log rather than retrying
            verify_checksum=True,
            encrypt_data=True,
            sync_level="checksum"
        )

        for file in files:
            td.add_item(
                file.globus_fn,
                str(Path(self._dest_root_path) / file.local_path / file.filename),
            )
        return td

    async def _parse_success_result(self, task_status: GlobusHTTPResponse) -> GlobusTaskResult:
        assert task_status.data['status'] == 'SUCCEEDED'

        if task_status.data['subtasks_skipped_errors']:
            # Note: refer user to globus transfer web logs if they want to know skip REASON
            skip_resp = self._client.paginated.task_skipped_errors(self._transfer_id)
            skipped_paths = {f['source_path'] for f in skip_resp.items()}
            results = [
                FileResult(FileStatus.Done if path not in skipped_paths else FileStatus.Error, f)
                for f in self._files
                for path in (f.globus_path,)
            ]
        else:
            # If it wasn't skipped due to an error, then assume it was successfully transferred
            results = [FileResult(FileStatus.Done, f) for f in self._files]

        return GlobusTaskResult(
            self._task_label,
            results,
            'Transfer succeeded',
            self._start_time,
            globus_task_id=self._transfer_id,
        )


    ######## ABC implementation
    async def _pre_check(self, files: list[File]) -> tuple[list[File], list[FileResult]]:
        """The globus transfer result will automatically handle skip logic and missing files."""
        return files, []

    async def _check_transfer_status(self, resp: GlobusHTTPResponse) -> GlobusTaskResult | None:
        """
        Interpret one transfer status response.

        Returns a GlobusTaskResult for terminal states (SUCCEEDED, FAILED),
        or None if the transfer is still in progress (ACTIVE, INACTIVE).
        """
        # TODO verify- since some files may be skipped, checksummed may be the most useful metric for progress bar
        n_files_completed = resp.data['files_skipped'] + resp.data['files_transferred']
        bytes_completed = resp.data['bytes_checksummed']
        self._emit_heartbeat(n_files_completed, bytes_completed)

        status = _GlobusTransferStatus[resp.data['status']]
        match status:
            case _GlobusTransferStatus.ACTIVE | _GlobusTransferStatus.INACTIVE:
                # FIXME: slightly tweak this branch for clarity
                return None
            case _GlobusTransferStatus.FAILED:
                return self.to_fail(msg="The transfer has failed")
            case _GlobusTransferStatus.SUCCEEDED:
                return await self._parse_success_result(resp)

    async def _run(self, items: list[File], skip: list[FileResult]) -> TaskResult:
        if self._transfer_id is None:
            td = self._make_transfer_data(items)
            resp = self._client.submit_transfer(td)
            self._transfer_id = resp.data['task_id']

            start_info = GlobusTaskStartInfo(self._task_label, self._start_time, items, skip, self._transfer_id)
            self._emit_start(start_info)
        else:
            logging.info(f'Checking existing Globus transfer task: {self._transfer_id}')

        if self._wait_until_resolved:
            while True:
                resp = self._client.get_task(self._transfer_id)  # alas, globus sdk doesn't have async variants
                result = await self._check_transfer_status(resp)
                if result is not None:
                    return result
                # TODO client svc creds shouldn't expire; revisit inactive handling when we add user login mode
                max_poll_time = 60 * 10
                if self._poll_time < max_poll_time:
                    self._poll_time = max(self._poll_time + 15, max_poll_time)
                await asyncio.sleep(self._poll_time)
        else:
            resp = self._client.get_task(self._transfer_id)
            result = await self._check_transfer_status(resp)
            if result is not None:
                return result
            # Transfer still in progress; report all files as Started
            fr = [FileResult(FileStatus.Started, f) for f in items]
            return GlobusTaskResult(
                self._task_label,
                fr,
                'Transfer in progress',
                self._start_time,
                globus_task_id=self._transfer_id,
            )

    async def _post_check(self, result: TaskResult) -> TaskResult:
        """
        The globus transfer service already verifies checksum integrity, and we don't attempt to guard
            against the catalog advertising a hash different than the file actually available
        """
        return result

    async def _cleanup(self, result: TaskResult) -> TaskResult:
        """
        For now, we explicitly do not perform cleanup, because a partially transferred failure uses globus sync mode
            to reduce data moved on retry
        """
        return result
