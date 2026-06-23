from __future__ import annotations

from abc import ABC
import asyncio
from pathlib import Path
from typing import Optional, TYPE_CHECKING, TypedDict

from globus_sdk import TransferData, GlobusAPIError, NetworkError

from esgpull.models import File, GlobusTransferStatus

if TYPE_CHECKING:
    from globus_sdk import TransferClient

from esgpull.downloader.base import (
    DownloadTask,
    FileResult,
    TaskResultEvent,
    TaskStatus,
)
from esgpull.models.file import FileStatus
from esgpull.tui import logger


class GlobusTaskCommon(DownloadTask, ABC):
    """Common behaviors for all Globus task types"""
    _transfer_task_id: Optional[str] = None
    _globus_task_status: Optional[GlobusTransferStatus] = None

    def __init__(
            self,
            task_label: str,
            files: list[File],
            client: TransferClient
    ):
        super().__init__(task_label, files)
        self._client = client

    def _get_extra(self) -> dict:
        return {
            "globus_task_id": self._transfer_task_id,
            "globus_task_status": self._globus_task_status
        }

    async def _pre_check(self, files: 'list[File]') -> 'tuple[list[File], list[FileResult]]':
        """The globus transfer service will automatically handle skip logic and missing files."""
        return files, []

    async def _cleanup(self, result: TaskResultEvent) -> TaskResultEvent:
        """
        No cleanup: on retry of partial failure, we use sync mode to minimize data moved
        """
        return result

    ########## Internal helpers
    def to_cancel(self) -> TaskResultEvent:
        """
        If a globus transfer is canceled after the task was submitted, then those files are still in progress and
            status can be resolved next time the program is run
        """
        fr = [FileResult(FileStatus.Started, f) for f in self._files]
        return self._make_result(
            TaskStatus.CANCELED,
            'Transfer cancelled before submission',
            fr
        )


class GlobusStatusTask(GlobusTaskCommon):
    """Check the status of an existing globus transfer"""
    def __init__(
        self,
        task_label: str,
        files: list[File],
        client: TransferClient,

        transfer_task_id: str,  # the task ID from the globus transfer api

        wait_until_resolved: bool = True,
        poll_time_max: int = 60 * 90
    ):
        super().__init__(task_label, files, client)
        self._transfer_task_id = transfer_task_id
        self._wait_until_resolved = wait_until_resolved
        self._poll_time = 60  # Hardcoded minimum, gradually increases to configured max
        self._poll_time_max = poll_time_max

    ### Overrides
    def _emit_start(self, *args, **kwargs) -> None:
        """Status checks do not emit a start event; by definition they are already in progress"""
        pass

    ### Helpers
    async def _check_transfer_status(self) -> tuple[GlobusTransferStatus, bool]:
        assert self._transfer_task_id is not None
        resp = self._client.get_task(self._transfer_task_id)  # signoff: globus sdk doesn't support async
        status = GlobusTransferStatus[resp.data['status']]
        has_skipped = resp.data['subtasks_skipped_errors'] != 0

        n_files_completed = resp.data['files_skipped'] + resp.data['files_transferred']
        bytes_completed = resp.data['bytes_checksummed']

        self._globus_task_status = status
        self._emit_heartbeat(n_files_completed, bytes_completed)

        return status, has_skipped

    async def _poll_for_completion(self) -> tuple[GlobusTransferStatus, bool]:
        while True:
            status, has_skipped = await self._check_transfer_status()
            if status in GlobusTransferStatus.resolved():
                return status, has_skipped

            if self._poll_time < self._poll_time_max:
                self._poll_time = min(self._poll_time + 15, self._poll_time_max)
            await asyncio.sleep(self._poll_time)

    async def _check_skipped_errors(self) -> set[str]:
        """A successful globus task may skip some files due to errors. Record status correctly."""
        skip_resp = self._client.paginated.task_skipped_errors(self._transfer_task_id)
        return {f['source_path'] for f in skip_resp.items()}

    ### Implementation
    def _handle_globus_exc(self, e: GlobusAPIError | NetworkError) -> TaskResultEvent:
        if isinstance(e, GlobusAPIError):
            if e.http_status == 404:
                # The task ID is gone (expired or otherwise invalid): no amount of retrying the status
                # check will ever resolve it, so don't leave files stuck in Started — mark for retry.
                logger.error(
                    "Globus task %s not found — transfer can never be resolved", self._transfer_task_id
                )
                return self.to_fail(f"Globus task {self._transfer_task_id} not found")
            if e.http_status in (401, 403):
                logger.error(
                    "Globus authorization error (%s) for task %s — re-authentication required",
                    e.http_status, self._transfer_task_id
                )
            else:
                logger.warning("Globus API error (%s) checking task %s", e.http_status, self._transfer_task_id)
            return self.to_unknown(f"Globus API error ({e.http_status})")
        else:
            logger.warning("Globus API unreachable checking task %s: %s", self._transfer_task_id, e)
            return self.to_unknown("Globus API unreachable")

    async def _run(self, to_download: list[File], skip: list[FileResult], **kwargs) -> TaskResultEvent:
        try:
            if not self._wait_until_resolved:
                status, has_skipped = await self._check_transfer_status()
            else:
                status, has_skipped = await self._poll_for_completion()
        except (GlobusAPIError, NetworkError) as e:
            return self._handle_globus_exc(e)

        if status == GlobusTransferStatus.FAILED:
            return self.to_fail(msg="Globus transfer failed")
        elif status in GlobusTransferStatus.running():
            return self._make_result(
                TaskStatus.ACTIVE,
                "Transfer is in progress",
                [FileResult(FileStatus.Started, f) for f in self._files],
            )

        # Handle tasks that succeeded, and check list of completed files
        try:
            skipped = await self._check_skipped_errors() if has_skipped else set()
        except (GlobusAPIError, NetworkError) as e:
            return self._handle_globus_exc(e)

        fr = [
            FileResult(FileStatus.Error if f.globus_fn in skipped else FileStatus.Done, f)
            for f in to_download
        ]
        return self._make_result(TaskStatus.SUCCESS, 'The transfer succeeded', fr)


class GlobusTransferTask(GlobusTaskCommon):
    """
    Transfer a batch of files to a Globus endpoint, or poll an existing transfer.
    """
    def __init__(
        self,
        task_label: str,
        files: 'list[File]',
        client: 'TransferClient',

        source_collection_id: Optional[str] = None,
        dest_collection_id: Optional[str] = None,
        dest_root_path: Optional[str] = None,

        wait_until_resolved: bool = True,
        poll_time: int = 60,
    ) -> None:
        super().__init__(task_label, files, client)

        self._source_collection_id = source_collection_id
        self._dest_collection_id = dest_collection_id
        self._dest_root_path = dest_root_path

        self._wait_until_resolved = wait_until_resolved
        self._poll_time = poll_time

    def _make_transfer_data(self, files: 'list[File]') -> TransferData:
        assert self._source_collection_id is not None
        assert self._dest_collection_id is not None
        assert self._dest_root_path is not None

        td = TransferData(
            self._source_collection_id,
            self._dest_collection_id,
            skip_source_errors=True,  # if a file doesn't exist, we'll capture from event log rather than retrying
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

    ######## ABC implementation
    async def _setup(self, to_download: list[File]):
        """
        Must submit transfer in setup step so that the globus task ID can be emitted in start event"""
        td = self._make_transfer_data(to_download)

        resp = self._client.submit_transfer(td)
        self._transfer_task_id = resp.data['task_id']


    async def _run(self, items: list[File], skip: list[FileResult]) -> TaskResultEvent:
        assert self._transfer_task_id is not None
        if not self._wait_until_resolved:
            return self._make_result(
                TaskStatus.ACTIVE,
                "The globus transfer will run in the background",
                [FileResult(FileStatus.Started, f) for f in items]
            )

        proxy = GlobusStatusTask(
            self._task_label,
            items,
            self._client,
            self._transfer_task_id,
            self._wait_until_resolved,
            self._poll_time
        )
        return await proxy.run()
