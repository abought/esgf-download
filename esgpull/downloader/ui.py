"""
Rich-based progress UI for HTTPS downloads.

Rendering and per-file completion text are kept byte-for-byte identical to the
legacy implementation for backward compatibility.
"""
from __future__ import annotations

import contextlib
from typing import Iterator

from rich.live import Live
from rich.progress import (
    BarColumn,
    DownloadColumn,
    MofNCompleteColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from esgpull.downloader.base import TaskResultEvent, TaskStartEvent, TaskHeartbeatEvent
from esgpull.models import File, FileStatus
from esgpull.models.utils import short_sha
from esgpull.tui import UI, DummyLive, ErrorCountColumn, logger
from esgpull.utils import format_size


class HttpsDownloadUI:
    """Progress bar UI for a group of url-based file downloads"""

    def __init__(
            self,
            ui: UI,
            queue_size: int,
            show_filename: bool,
            show_progress: bool = True
    ) -> None:
        self._app_ui = ui
        self._show_filename = show_filename
        self._show_progress = show_progress

        self.main_progress = ui.make_progress(
            SpinnerColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(compact=True, elapsed_when_finished=True),
            ErrorCountColumn(),
        )
        file_columns: list[str | ProgressColumn] = [
            TextColumn("[cyan][{task.id}] [b blue]{task.fields[sha]}"),
            "[progress.percentage]{task.percentage:>3.0f}%",
            BarColumn(),
            "·",
            DownloadColumn(binary_units=True),
            "·",
            TransferSpeedColumn(),
            "·",
            TextColumn("[blue]{task.fields[data_node]}"),
        ]
        if show_filename:
            file_columns.extend(["·", TextColumn("{task.fields[filename]}")])
        self.file_progress = ui.make_progress(*file_columns, transient=True)

        self._task_ids: dict[str, TaskID] = {}
        self._nb_errors = 0
        self._queue_remaining = queue_size
        self._main_task_id = self.main_progress.add_task("", total=queue_size, nb_errors=0)
        self._live: Live | DummyLive | None = None

    def add_task(self, file: File) -> TaskID:
        task_id = self.file_progress.add_task(
            "",
            total=file.size,
            visible=False,
            start=False,
            sha=short_sha(file.sha),
            filename=file.filename,
            data_node=file.data_node,
        )
        self._task_ids[file.file_id] = task_id
        return task_id

    def on_start(self, event: TaskStartEvent) -> None:
        for file in event.files:
            task_id = self.add_task(file)
            self.file_progress.start_task(task_id)
            self.file_progress.update(task_id, visible=True)
        for _ in event.already_done:
            self.main_progress.update(self._main_task_id, advance=1)

    def on_heartbeat(self, event: TaskHeartbeatEvent) -> None:
        task_id = self._task_ids.get(event.task_label)
        if task_id is not None:
            self.file_progress.update(task_id, completed=event.bytes_completed)

    def on_result(self, event: TaskResultEvent) -> None:
        task_id = self._task_ids.pop(event.task_label, None)
        task = None
        if task_id is not None:
            task_idx = self.file_progress.task_ids.index(task_id)
            task = self.file_progress.tasks[task_idx]
            self.file_progress.stop_task(task_id)
            self.file_progress.update(task_id, visible=False)

        # HTTPS tasks bundle exactly one file (factory.make_https_tasks), so
        # `event.files` has a single entry; its FileStatus — not the task-level
        # event.status, which stays SUCCESS even when a per-file download
        # fails inside HttpsDownloadTask._run() — is what determines success.
        succeeded = any(fr.status == FileStatus.Done for fr in event.files)
        if succeeded:
            self.main_progress.update(self._main_task_id, advance=1)
            if task is not None:
                self._print_completion_line(task)
        else:
            self._nb_errors += 1
            self._queue_remaining -= 1
            self.main_progress.update(
                self._main_task_id,
                total=self._queue_remaining,
                nb_errors=self._nb_errors,
            )

        if task_id is not None:
            self.file_progress.remove_task(task_id)

    def _print_completion_line(self, task) -> None:
        sha = f"[b blue]{task.fields['sha']}[/]"
        size = f"[green]{format_size(int(task.completed))}[/]"
        if task.elapsed:
            final_speed = int(task.completed / task.elapsed)
            speed = f"[red]{format_size(final_speed)}/s[/]"
        else:
            speed = "[b red]?[/]"
        data_node = f"[blue]{task.fields['data_node']}[/]"
        parts = [sha, size, speed, data_node]
        if self._show_filename:
            parts.append(task.fields["filename"])
        msg = " · ".join(parts)
        logger.info(msg)
        if self._live is not None:
            self._live.console.print(msg)

    @contextlib.contextmanager
    def live(self) -> Iterator[Live | DummyLive]:
        with self._app_ui.live(
            self.file_progress,
            self.main_progress,
            disable=not self._show_progress,
        ) as live:
            self._live = live
            try:
                yield live
            finally:
                self._live = None
