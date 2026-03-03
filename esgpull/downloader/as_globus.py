import asyncio
import dataclasses
import enum
from pathlib import Path
from typing import Optional, TYPE_CHECKING

from globus_sdk import TransferData, GlobusHTTPResponse

if TYPE_CHECKING:
    from globus_sdk import TransferClient

from esgpull import File
from esgpull.downloader.base import (
    DownloadTask,
    FileResult,
    FileStatus,
    TaskHeartbeat,
    TaskResult,
    TaskStartInfo,
)


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


            poll_time: int = 60 * 1
    ) -> None:
        super().__init__(task_label, files)

        self._client = client
        self._source_collection_id = source_collection_id

        self._dest_collection_id = dest_collection_id
        self._dest_root_path = dest_root_path

        # For long transfers, default poll time will increase up to a system-provided max
        self._poll_time = poll_time

        # TODO: implement a way to skip start setup if this is provided, maybe consolidate start and result methods accordingly?
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
                FileResult(FileStatus.SUCCESS if path not in skipped_paths else FileStatus.FAIL, f)
                for f in self._files
                for path in (f.globus_path,)
            ]
        else:
            # If it wasn't skipped due to an error, then assume it was successfully transferred
            results = [FileResult(FileStatus.SUCCESS, f) for f in self._files]

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

    async def _run(self, items: list[File], skip: list[FileResult]) -> TaskResult:
        if self._transfer_id is None:
            td = self._make_transfer_data(items)
            resp = self._client.submit_transfer(td)
            self._transfer_id = resp.data['task_id']

        start_info = GlobusTaskStartInfo(self._task_label, self._start_time, items, skip, self._transfer_id)
        self._emit_start(start_info)

        while True:
            resp = self._client.get_task(self._transfer_id)  # alas, globus sdk doesn't have async variants
            status = _GlobusTransferStatus[resp.data['status']]  # ACTIVE can include Queued , so this could be set to poll less often

            # TODO verify- since some files may be skipped, checksummed may be the most useful metric for progress bar
            n_files_completed = resp.data['files_skipped'] + resp.data['files_transferred']
            bytes_completed = resp.data['bytes_checksummed']

            self._emit_heartbeat(n_files_completed, bytes_completed)

            match status:
                case _GlobusTransferStatus.ACTIVE | _GlobusTransferStatus.INACTIVE:
                    # TODO client svc creds shouldn't expire; revisit inactive handling when we add user login mode, may want extra warnings then
                    max_poll_time = 60 * 10
                    if self._poll_time < max_poll_time:
                        self._poll_time = max(self._poll_time + 15, max_poll_time)
                    await asyncio.sleep(self._poll_time)
                case _GlobusTransferStatus.FAILED:
                    return self.to_fail(msg="The transfer has failed")
                case _GlobusTransferStatus.SUCCEEDED:
                    # A transfer can be a partial success; examine this state more carefully.
                    return await self._parse_success_result(resp)

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
