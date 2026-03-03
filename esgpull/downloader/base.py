"""
Base classes for different download methods
"""
from abc import ABC, abstractmethod
import dataclasses
from datetime import datetime, timezone
from enum import IntEnum, auto
from typing import Callable, Optional

from esgpull.models import File

############
# Status tracking for task lifecycle events
class FileStatus(IntEnum):
    FAIL = auto()
    SUCCESS = auto()


@dataclasses.dataclass(frozen=True)
class FileResult:
    """Download result for one file"""
    status: FileStatus
    file: File

    @classmethod
    def fail_all(cls, files: list[File]) -> 'list[FileResult]':
        """Helper for generic exception handling"""
        return [cls(FileStatus.FAIL, f) for f in files]


@dataclasses.dataclass
class TaskResult:
    """
    Report the result of the overall task, which may include multiple files

    In a batch like globus transfer, it's possible that a task can succeed but some individual files may fail
    """
    task_label: str

    files: list[FileResult]

    msg: str

    start_time: datetime
    end_time: datetime = datetime.now(timezone.utc)


@dataclasses.dataclass
class TaskStartInfo:
    """
    Track state for a task that has started. Can be used to return eg, an async globus task ID
    """
    task_label: str

    started_at: datetime
    files: list[File]
    already_done: list[FileResult]  # files that are considered complete before the task even starts


StartCallback = Callable[[TaskStartInfo], None]


@dataclasses.dataclass
class TaskHeartbeat:
    """Heartbeat events can be used to guide progress bars"""
    task_label: str

    n_files_completed: int
    n_files_expected: int

    bytes_completed: int
    bytes_expected: int

    event_time: datetime = datetime.now(timezone.utc)

HeartbeatCallback = Callable[[TaskHeartbeat], None]


##############

class DownloadTask(ABC):
    """
    A generic downloader task for one or more files. Tasks should be as granular as possible to facilitate reporting:
        eg one file download, one batch globus transfer
    """
    def __init__(
            self,
            task_label: str,
            files: list[File]
    ) -> None:
        self._task_label = task_label  # unique label, used for monitoring task progress

        self._files = files
        self._to_download: list[File] = []  # used for uncaught exceptions

        self._files_expected = len(files)
        self._bytes_expected = sum(f.size for f in files)

        self._start_time: Optional[datetime] = None

        self._heartbeat_callbacks: list[HeartbeatCallback] = []
        self._start_callbacks: list[StartCallback] = []

    #########
    # Internal helpers
    def _emit_heartbeat(self, files_completed: int, bytes_completed: int) -> None:
        event = TaskHeartbeat(
            self._task_label,
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

    def to_result(self, msg, file_result: list[FileResult]) -> TaskResult:
        return TaskResult(
            self._task_label,
            file_result,
            msg,
            self._start_time,
        )

    def to_fail(self, msg: str = "An unknown error occurred") -> TaskResult:
        """
        Helper method for unhandled exceptions: task result should convey a failed file result for all files
        """
        items = self._to_download or self._files
        fr = [
            FileResult(FileStatus.FAIL, f)
            for f in items
        ]
        return TaskResult(
            self._task_label,
            fr,
            msg,
            self._start_time,
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
    async def _run(self, to_download: list[File], skip: list[FileResult]) -> TaskResult:
        """
        Execute the download and return its result.

        Implementations are responsible for:
          - emitting a start event via _emit_start (with a TaskStartInfo or subclass)
          - emitting heartbeat events via _emit_heartbeat as progress is made
          - returning a TaskResult when the transfer completes or fails
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
    async def run(self) -> TaskResult:
        """
        Execute the full task lifecycle: pre-check, download, post-check, and cleanup.

        Emits a start event (with task-specific info) at the beginning of the transfer,
        and heartbeat events during download. Both are preserved under their original names.

        NOTE: Some task types (eg Globus) may not resolve within the same process run.
            A task submitted with a known transfer_id can be re-instantiated to resume polling.
        """
        self._start_time = datetime.now(timezone.utc)

        to_download, skip = await self._pre_check(self._files)
        self._to_download = to_download

        # FIXME: add exception handling mechanism
        run_result = await self._run(to_download, skip)

        validated = await self._post_check(run_result)
        await self._cleanup(validated)

        return run_result
