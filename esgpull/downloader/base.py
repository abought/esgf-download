"""
Base classes for different download methods
"""
from abc import ABC, abstractmethod
import dataclasses
from datetime import datetime
from enum import IntEnum, auto
from typing import Callable, Generic, Optional, TypeVar

from esgpull.models import File

############
# Status tracking for task lifecycle events
class FileStatus(IntEnum):
    FAIL = auto()
    SUCCESS = auto()


@dataclasses.dataclass
class FileResult:
    """Download result for one file"""
    ok: bool
    status: FileStatus

    file_id: str
    local_path: str

@dataclasses.dataclass
class TaskResult:
    """
    Report the result of the overall task, which may include multiple files

    In a batch like globus transfer, it's possible that a task can succeed but some individual files may fail
    """
    ok: bool
    files: list[FileResult]

    start_time: datetime
    end_time: datetime

    msg: str


@dataclasses.dataclass
class TaskStartInfo:
    """
    Track state for a task that has started. Can be used to return eg, an async globus task ID
    """
    task_id: str

    started_at: datetime
    already_done: list[FileResult]  # files that are considered complete before the task even starts


S = TypeVar("S", bound=TaskStartInfo)

StartCallback = Callable[[TaskStartInfo], None]


@dataclasses.dataclass
class TaskHeartbeat:
    """Heartbeat events can be used to guide progress bars"""
    task_id: str

    files_completed: int
    files_expected: int

    bytes_completed: int
    bytes_expected: int

    event_time: datetime = datetime.now()

HeartbeatCallback = Callable[[TaskHeartbeat], None]


##############

class DownloadTask(ABC, Generic[S]):
    """
    A generic downloader task for one or more files. Tasks should be as granular as possible to facilitate reporting:
        eg one file download, one batch globus transfer
    """
    def __init__(
            self,
            task_id: str,
            files: list[File]
    ) -> None:
        self._task_id = task_id  # unique label, used for monitoring task progress

        self._files = files

        self._files_expected = len(files)
        self._bytes_expected = sum(f.size for f in files)

        self._start_time: Optional[datetime] = None
        self._to_download: list[File] = []

        self._heartbeat_callbacks: list[HeartbeatCallback] = []
        self._start_callbacks: list[StartCallback] = []

    #########
    # Internal helpers
    def _emit_heartbeat(self, files_completed: int, bytes_completed: int) -> None:
        event = TaskHeartbeat(
            self._task_id,
            files_completed,
            self._files_expected,
            bytes_completed,
            self._bytes_expected
        )
        for callback in self._heartbeat_callbacks:
            callback(event)

    def _emit_start(self, start_info: TaskStartInfo) -> None:
        for callback in self._start_callbacks:
            callback(start_info)

    def _fail_all(self, msg: str) -> TaskResult:
        """
        Helper method for unhandled exceptions: task result should convey a failed file result for all files
        """
        fr = [
            FileResult(False, FileStatus.FAIL, f.file_id, f.local_path)
            for f in self._files
        ]
        return TaskResult(
            True,
            fr,
            self._start_time,
            datetime.now(),
            msg = msg | 'An unknown error occurred'
        )

    ########
    # Critical steps of the task lifecycle
    @abstractmethod
    async def _pre_check(self, files: list[File]) -> tuple[list[File], list[FileResult]]:
        """
        Decide which file(s) should be downloaded.

        Returns a tuple of (valid_files_pending, invalid_files_result)

        How the result is presented is up to the implementation.
        For example, if a file already exists locally, it may be reported as a successful download
        """
        raise NotImplementedError

    @abstractmethod
    async def _post_check(self, result: TaskResult) -> TaskResult :
        """
        Check if the file(s) downloaded correctly.

        For example, this might entail validating checksums of what was transferred

        Returns a tuple of (valid_file_results, invalid_file_results)

        How the result is presented is up to the implementation.

        """
        raise NotImplementedError

    @abstractmethod
    async def _cleanup(self, result: TaskResult) -> None:
        """
        Perform any necessary cleanup. For example, if a file fails checksum validation after download,
            it might be deleted from the local copy
        """
        pass

    @abstractmethod
    async def _start(self, items: list[File]) -> S:
        """
        Start the download task and report back.

        Eg, might report a submitted globus task_id, which can be resolved later even if the esgpull process
            is interrupted
        """
        raise NotImplementedError


    @abstractmethod
    async def _result(self, items: list[File]) -> TaskResult:
        """Report the result"""
        raise NotImplementedError


    #########
    # Public API

    # Event listeners
    def on_heartbeat(self, callback: HeartbeatCallback) -> None:
        """
        Tasks *should* emit a heartbeat event for progress bar tracking
        """
        if callback not in self._heartbeat_callbacks:
            self._heartbeat_callbacks.append(callback)

    def on_start(self, callback: StartCallback) -> None:
        """
        Tasks *will* emit a start event that can be used for customized per-task behavior. For generic listeners, consider defining once at the orchestrator level
        """
        if callback not in self._start_callbacks:
            self._start_callbacks.append(callback)


    # Task processing
    async def start(self) -> S:
        """
        Start the task by performing necessary setup and validation. Must be called to prepare any new transfer.

        See also: result()
        """
        self._start_time = datetime.now()

        to_download, skip = await self._pre_check(self._files)

        self._to_download = to_download

        start_info = await self._start(to_download)
        start_info.already_done = skip

        self._emit_start(start_info)
        return start_info

    async def result(self):
        """
        Return the result of the task.

        TODO: Improve interface to handle case of "checking a globus task in next process run"
            --> Must either have called start here, OR, be checking a previously known task ID

        NOTE: Some task types may not call `result()` during the same run of the esgpull process.
            Eg a globus transfer task happens async outside the process, and a cron job might prefer to fire-and-forget
        """
        if not self._start_time:
            raise Exception('The download task has not started yet.')

        run_result = await self._result(self._to_download)

        validated = await self._post_check(run_result)
        await self._cleanup(validated)

        return run_result

