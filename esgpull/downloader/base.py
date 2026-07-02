"""
Base classes for different download methods
"""
from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import enum
from typing import Any, Callable, Optional

from esgpull.models import File
from esgpull.models.file import FileStatus


class TaskStatus(enum.IntEnum):
    ACTIVE = 0
    CANCELED = 1
    COMPLETE = 2
    FAIL = 3
    UNKNOWN = 4  # status could not be determined due to transient issues

@dataclass(frozen=True)
class FileResult:
    """Download result for one file"""
    status: FileStatus
    file: File
    msg: str = ""

    @classmethod
    def fail_all(cls, files: list[File]) -> 'list[FileResult]':
        """Helper for generic exception handling"""
        return [cls(FileStatus.Error, f) for f in files]


@dataclass
class TaskResultEvent:
    """
    Report the result of the overall task, which may include multiple files
    """
    task_label: str
    status: TaskStatus
    msg: str

    files: list[FileResult]  # Omits `already_done` files (per TaskStartInfo) and lists only new work done

    # Task subtypes can provide extra info, like linked task IDs
    extra: dict

    start_time: Optional[datetime]  # blank if task canceled before start
    end_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    exception: Optional[BaseException] = field(default=None)

ResultCallback = Callable[[TaskResultEvent], None]

@dataclass(frozen=True)
class TaskStartEvent:
    """
    Track state for a task that has started. May be extended per task type
    """
    task_label: str

    # Files that will be processed
    files: list[File]
    # Files that are considered complete and will be skipped
    already_done: list[FileResult]

    extra: dict

    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


StartCallback = Callable[[TaskStartEvent], None]


@dataclass(frozen=True)
class TaskHeartbeatEvent:
    """Heartbeat events can be used to guide progress bars"""
    task_label: str

    n_files_completed: int
    n_files_expected: int

    bytes_completed: int
    bytes_expected: int

    extra: dict

    event_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

HeartbeatCallback = Callable[[TaskHeartbeatEvent], None]


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
        self._result_callbacks: list[ResultCallback] = []

    #########
    # Internal helpers
    def _get_extra(self):
        """Custom task types can provide "extra" fields, like a task ID"""
        return {}

    def _emit_heartbeat(self, files_completed: int, bytes_completed: int) -> None:
        event = TaskHeartbeatEvent(
            self._task_label,
            files_completed,
            self._files_expected,
            bytes_completed,
            self._bytes_expected,
            self._get_extra()
        )
        for callback in self._heartbeat_callbacks:
            callback(event)

    def _emit_start(self, to_download: list[File], skip: list[FileResult]) -> None:
        assert self._start_time is not None
        event = TaskStartEvent(
            self._task_label,
            to_download,
            skip,
            self._get_extra(),
            self._start_time
        )
        for callback in self._start_callbacks:
            callback(event)

    def _make_result(self, status: TaskStatus, msg, file_results: list[FileResult], exception: Optional[BaseException] = None):
        return TaskResultEvent(
            self._task_label,
            status,
            msg,
            file_results,
            self._get_extra(),
            self._start_time,
            exception=exception,
        )

    def _emit_result(self, event: TaskResultEvent):
        for callback in self._result_callbacks:
            callback(event)

    @abstractmethod
    def to_cancel(self) -> TaskResultEvent:
        """
        Define how to record status if the program is interrupted.
        """
        raise NotImplementedError

    def to_unknown(self, msg: str = "Transfer status could not be determined", exception: Optional[BaseException] = None) -> TaskResultEvent:
        items = self._to_download or self._files
        fr = [FileResult(FileStatus.Started, f) for f in items]
        return self._make_result(TaskStatus.UNKNOWN, msg, fr, exception=exception)

    def to_fail(self, msg: str = "An unknown error occurred", exception: Optional[BaseException] = None) -> TaskResultEvent:
        """
        The task definitively failed. All files are marked Error.
        """
        items = self._to_download or self._files
        fr = FileResult.fail_all(items)
        return self._make_result(TaskStatus.FAIL, msg, fr, exception=exception)

    ########
    # Critical steps of the task lifecycle
    @abstractmethod
    async def _pre_check(self, files: list[File]) -> tuple[list[File], list[FileResult]]:
        """
        Decide which file(s) should be downloaded.

        Returns a tuple of (valid_files_pending, already_resolved_files)

        How the "resolved" result is presented is up to the implementation.
        For example, if a file already exists locally, it may be reported as a successful download.
        """
        raise NotImplementedError

    async def _setup(self, to_download: list[File]):
        """
        Allow operations to occur before the start event is sent
        These operations can have side effects, like setting data fields on the instance
        """
        pass

    @abstractmethod
    async def _run(self, to_download: list[File], skip: list[FileResult]) -> TaskResultEvent:
        """
        Execute the download and return its result.

        Implementations are responsible for:
          - emitting a start event via _emit_start (with a TaskStartInfo or subclass)
          - emitting heartbeat events via _emit_heartbeat as progress is made
          - returning a TaskResult when the transfer completes or fails
        """
        raise NotImplementedError

    @abstractmethod
    async def _cleanup(self, result: TaskResultEvent) -> TaskResultEvent:
        """
        Perform any necessary cleanup steps. (deleting temp files, moving to final location, etc)
        """
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

    def on_result(self, callback: ResultCallback):
        """
        Exactly mirrors the return value of `task.run()`, but in a way that allows type-specific end behavior
            (outside of the task: like cleaning up DB records of globus transfer tasks)

        Unlike task.run(), callbacks are not guaranteed to fire if a task fails or is canceled
        """
        if callback not in self._result_callbacks:
            self._result_callbacks.append(callback)

    def on_start(self, callback: StartCallback) -> None:
        """
        Tasks *will* emit a start event that can be used for customized per-task behavior.
            For generic listeners, consider defining once at the orchestrator level
        """
        if callback not in self._start_callbacks:
            self._start_callbacks.append(callback)

    def _forward_heartbeats_to(self, other: 'DownloadTask') -> None:
        """Forward this task's heartbeat listeners to a delegated sub-task."""
        for cb in self._heartbeat_callbacks:
            other.on_heartbeat(cb)

    # Task processing
    async def run(self) -> TaskResultEvent:
        """
        Execute the full task lifecycle: pre-check, download, and cleanup.

        Emits a start event (with task-specific info) at the beginning of the transfer,
        and heartbeat events during download.
        """
        self._start_time = datetime.now(timezone.utc)

        to_download, skip = await self._pre_check(self._files)
        self._to_download = to_download

        try:
            await self._setup(to_download)  # side-effecty prepare any info needed for start event
        except Exception as e:
            # Nothing was actually started (eg the remote service rejected submission), so there's
            # no start event to emit. Report it the same way the orchestrator's own catch-all would.
            final = await self._cleanup(self.to_fail(str(e), exception=e))
            self._emit_result(final)
            return final

        self._emit_start(to_download, skip)

        try:
            run_result = await self._run(to_download, skip)
            final = await self._cleanup(run_result)
        except (asyncio.CancelledError, KeyboardInterrupt):
            # Run cleanup (eg delete partial downloads) before propagating.
            # The worker's exception handler is responsible for finalizing the result.
            await self._cleanup(self.to_cancel())
            raise

        self._emit_result(final)
        return final
