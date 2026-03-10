import asyncio
import dataclasses
import enum
from pathlib import Path
from typing import Optional, TYPE_CHECKING

from globus_sdk import TransferData, GlobusHTTPResponse

from esgpull.models import File, GlobusTransferStatus

if TYPE_CHECKING:
    from globus_sdk import TransferClient

from esgpull.downloader.base import (
    DownloadTask,
    FileResult,
    TaskHeartbeat,
    TaskResult,
    TaskStartInfo,
)
from esgpull.models.file import FileStatus
from esgpull.tui import logger


@dataclasses.dataclass
class GlobusTaskStartInfo(TaskStartInfo):
    globus_task_id: Optional[str]


@dataclasses.dataclass
class GlobusTaskHeartbeat(TaskHeartbeat):
    globus_task_id: str = dataclasses.field(kw_only=True)


@dataclasses.dataclass
class GlobusTaskResult(TaskResult):
    globus_task_id: Optional[str] = dataclasses.field(kw_only=True)
    globus_task_status: GlobusTransferStatus = dataclasses.field(kw_only=True)


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
    Transfer a batch of files to a Globus endpoint, or poll an existing transfer.

    Use the default constructor to submit a new transfer; use from_existing() to
    check the state of a transfer that was submitted in a prior run.
    """
    def __init__(
            self,
            task_label: str,
            files: 'list[File]',
            client: 'TransferClient',

            # Required for new transfers; unused (and may be None) in re-poll mode.
            source_collection_id: Optional[str] = None,
            dest_collection_id: Optional[str] = None,
            dest_root_path: Optional[str] = None,

            transfer_id: Optional[str] = None,

            poll_time: int = 60,
            wait_until_resolved: bool = True,
    ) -> None:
        if transfer_id is None:
            if source_collection_id is None or dest_collection_id is None or dest_root_path is None:
                raise ValueError(
                    "source_collection_id, dest_collection_id, and dest_root_path "
                    "are required when submitting a new transfer"
                )

        super().__init__(task_label, files)

        self._client = client
        self._source_collection_id = source_collection_id
        self._dest_collection_id = dest_collection_id
        self._dest_root_path = dest_root_path
        self._poll_time = poll_time
        self._wait_until_resolved = wait_until_resolved
        self._transfer_id = transfer_id

    @classmethod
    def from_existing(
            cls,
            task_label: str,
            files: 'list[File]',
            client: 'TransferClient',
            transfer_id: str,
            poll_time: int = 60,
            wait_until_resolved: bool = False,
    ) -> 'GlobusDownloadTask':
        """
        Check the state of a Globus transfer submitted in a prior run.
        Destination collection config is not needed since the transfer already
        exists on the Globus service.
        """
        return cls(
            task_label=task_label,
            files=files,
            client=client,
            transfer_id=transfer_id,
            poll_time=poll_time,
            wait_until_resolved=wait_until_resolved,
        )

    def to_cancel(self) -> GlobusTaskResult:
        if self._transfer_id is not None:
            # The transfer is running server-side and will continue after the local
            # process exits. Leave files in Started so check_transfers() can resolve them.
            fr = [FileResult(FileStatus.Started, f) for f in self._files]
            return GlobusTaskResult(
                self._task_label,
                fr,
                'Transfer interrupted locally; Globus transfer continues',
                self._start_time,
                globus_task_id=self._transfer_id,
                globus_task_status=GlobusTransferStatus.ACTIVE,
            )
        else:
            # Transfer was never submitted; treat as a local cancellation.
            fr = [FileResult(FileStatus.Cancelled, f) for f in self._files]
            return GlobusTaskResult(
                self._task_label,
                fr,
                'Transfer cancelled before submission',
                self._start_time,
                globus_task_id='',
                globus_task_status=GlobusTransferStatus.FAILED,
            )

    def to_fail(self, msg: str = "An unknown error occurred") -> GlobusTaskResult:
        fr = FileResult.fail_all(self._files)
        return GlobusTaskResult(
            self._task_label,
            fr,
            msg,
            self._start_time,
            globus_task_id=self._transfer_id,
            globus_task_status=GlobusTransferStatus.FAILED,
        )


    ########## Internal helpers
    def _emit_heartbeat(self, n_files_completed: int, bytes_completed: int) -> None:
        assert self._transfer_id
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

    def _make_transfer_data(self, files: 'list[File]') -> TransferData:
        assert self._source_collection_id is not None
        assert self._dest_collection_id is not None
        assert self._dest_root_path is not None

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
                FileResult(FileStatus.Done if f.globus_fn not in skipped_paths else FileStatus.Error, f)
                for f in self._files
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
            globus_task_status=GlobusTransferStatus.SUCCEEDED,
        )


    ######## ABC implementation
    async def _pre_check(self, files: 'list[File]') -> 'tuple[list[File], list[FileResult]]':
        """The globus transfer result will automatically handle skip logic and missing files."""
        return files, []

    def _make_start_info(self, to_download: list[File], skip: list[FileResult]) -> GlobusTaskStartInfo:
        return GlobusTaskStartInfo(self._task_label, self._start_time, to_download, skip, self._transfer_id)

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
            # FIXME: start event emitted before run, so task_id never included in start evenbt!!
            td = self._make_transfer_data(items)
            resp = self._client.submit_transfer(td)
            self._transfer_id = resp.data['task_id']
        else:
            logger.info(f'Checking existing Globus transfer task: {self._transfer_id}')

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
                globus_task_status=GlobusTransferStatus.ACTIVE,
            )

    async def _cleanup(self, result: TaskResult) -> TaskResult:
        """
        No cleanup: on retry of partial failure, we use sync mode to minimize data moved
        """
        return result
