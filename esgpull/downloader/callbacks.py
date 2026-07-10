"""
Generic, task-type-agnostic callbacks for tracking download state.

These are registered at the orchestrator level (not the task level) precisely because the
orchestrator guarantees they fire for every task result it produces — including results it
synthesizes itself on cancellation or an unhandled task exception — which a task's own
on_result is not guaranteed to see. Kept out of `esgpull.py` to keep that module focused on
orchestration flow rather than the mechanics of each callback.
"""
from __future__ import annotations

import errno
import os
from datetime import datetime, timezone

from globus_sdk import GlobusAPIError

from esgpull.database import Database
from esgpull.downloader.base import (
    ResultCallback,
    StartCallback,
    TaskResultEvent,
    TaskStartEvent,
    TaskStatus,
)
from esgpull.exceptions import GlobusLoginError, GlobusPermissionError
from esgpull.models import File, FileStatus, GlobusTransfer
from esgpull.result import Err
from esgpull.tui import logger


def track_file_state(db: Database, file: File, status: FileStatus, use_db: bool) -> None:
    file.status = status
    if use_db:
        db.add(file)


def process_task_result(
    db: Database,
    event: TaskResultEvent,
    use_db: bool,
) -> list[Err]:
    """
    When a download task completes, immediately update the DB state for that file
    Apply one orchestrator result to the DB; return Errs for any non-done files.

    NOTE: a task can finish with `TaskStatus.COMPLETE` even if one or all of its files failed.
        The database should reflect `File.Status` for each file in the task separately.
    """
    errors: list[Err] = []
    for fr in event.files:
        match fr.status:
            case FileStatus.Done:
                track_file_state(db, fr.file, FileStatus.Done, use_db)
            case FileStatus.Cancelled:
                track_file_state(db, fr.file, FileStatus.Cancelled, use_db)
            case FileStatus.Error:
                track_file_state(db, fr.file, FileStatus.Error, use_db)
                errors.append(Err(fr.file, RuntimeError(fr.msg or event.msg)))
            case FileStatus.Started:
                # Valid terminal state for Globus tasks: the remote transfer is running and will be checked later
                track_file_state(db, fr.file, FileStatus.Started, use_db)
            case _:
                # Unexpected state — may indicate a bug or external data corruption.
                raise RuntimeError(
                    f"Unexpected file status {fr.status!r} in completed task result"
                    f" for {fr.file.file_id} — halting downloads."
                    f" This may indicate a bug in the task implementation or external data corruption."
                )
    return errors


def make_on_task_start(db: Database, use_db: bool) -> StartCallback:
    def on_start(event: TaskStartEvent) -> None:
        for file in event.files:
            track_file_state(db, file, FileStatus.Starting, use_db)
        for fr in event.already_done:
            track_file_state(db, fr.file, FileStatus.Done, use_db)

    return on_start


def make_file_state_on_result(
    db: Database,
    use_db: bool,
    files: list[File],
    errors: list[Err],
) -> ResultCallback:
    """
    Common per-file DB state tracking, used by all download types
    """
    def on_result(event: TaskResultEvent) -> None:
        errors.extend(process_task_result(db, event, use_db))
        files.extend(fr.file for fr in event.files if fr.status == FileStatus.Done)

    return on_result


def log_download_errors(event: TaskResultEvent) -> None:
    """
    Log each per-file download error as it happens, rather than only at an
    end-of-run summary that an interrupted process may never reach.
    """
    for fr in event.files:
        if fr.status != FileStatus.Error:
            continue
        source = (
            f"globus:{fr.file.globus_storage.origin_id}"
            if fr.file.globus_storage is not None
            else fr.file.data_node
        )
        logger.error(
            f"  {fr.file.filename} [{source}]"
            f" [{fr.status.name}]: {fr.msg or event.msg}"
        )


def make_globus_transfer_on_result(db: Database) -> ResultCallback:
    """
    Track state of an async Globus Transfer task, separate from the files within
    """
    def on_result(event: TaskResultEvent) -> None:
        task_id = event.extra.get('globus_task_id')
        if task_id is None:
            return
        transfer = db.session.get(GlobusTransfer, task_id)
        if transfer is None:
            # TODO consider exception- this really should not be possible
            logger.error('A globus transfer task references a task_id that it not tracked in the database')
            return
        globus_status = event.extra.get('globus_task_status')
        if globus_status is not None:
            # If a globus task id is present, globus status should always be set
            transfer.status = globus_status
        transfer.last_updated = datetime.now(timezone.utc)
        transfer.completion_time = event.end_time
        db.add(transfer)

    return on_result


def check_globus_auth_error(event: TaskResultEvent, client_id: str = "") -> None:
    """Raise a specific GlobusAuthError subclass so the CLI can show a targeted message and exit with code 2."""
    if not event.exception or not isinstance(event.exception, GlobusAPIError):
        return
    exc = event.exception
    logger.debug(
        "Globus API error — status=%s code=%s request_id=%s message=%r errors=%r",
        exc.http_status, exc.code, exc.request_id, exc.message, exc.errors,
    )
    if exc.http_status == 401:
        raise GlobusLoginError(client_id)
    if exc.http_status == 403:
        raise GlobusPermissionError(client_id)


def is_disk_full(event: TaskResultEvent) -> bool:
    return event.status == TaskStatus.FAIL and os.strerror(errno.ENOSPC) in event.msg