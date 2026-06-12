"""
Unit tests for GlobusStatusTask and GlobusTransferTask.

NOTE: This file is a starting point and is expected to grow significantly once
real-world fixture payloads are captured (see design/globus_test_fixtures.md).
Several tests use simplified mock responses; those marked with TODO should be
revisited against actual Globus API payloads.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from esgpull.downloader.as_globus import GlobusStatusTask, GlobusTransferTask
from esgpull.downloader.base import TaskStatus
from esgpull.models import GlobusTransferStatus
from esgpull.models.file import FileStatus
from esgpull.models.globus_storage import GlobusStorage
from tests.downloader.fakes import FakeDownloadTask, make_file


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

def make_globus_file(file_id: str = "file", origin_path: str = "/esgf/data"):
    """Create a File with a GlobusStorage relationship so globus_fn is non-empty."""
    f = make_file(file_id=file_id)
    f.globus_storage = GlobusStorage(
        origin_id="source-collection-uuid",
        origin_path=origin_path,
    )
    return f


def task_response(
    status: str,
    files_transferred: int = 0,
    files_skipped: int = 0,
    bytes_checksummed: int = 0,
    subtasks_skipped_errors: int = 0,
) -> MagicMock:
    """Build a mock get_task() response with the fields _check_transfer_status reads."""
    resp = MagicMock()
    resp.data = {
        "status": status,
        "files_transferred": files_transferred,
        "files_skipped": files_skipped,
        "bytes_checksummed": bytes_checksummed,
        "subtasks_skipped_errors": subtasks_skipped_errors,
    }
    return resp


def submit_response(task_id: str = "transfer-task-uuid") -> MagicMock:
    resp = MagicMock()
    resp.data = {"task_id": task_id}
    return resp


def make_transfer_client(
    task_responses=None,
    submit_resp=None,
    skipped_paths=None,
) -> MagicMock:
    """
    Build a mock TransferClient.

    task_responses: list of get_task() return values (used as side_effect if >1).
    submit_resp: return value for submit_transfer(); defaults to a generic success.
    skipped_paths: list of source_path strings returned by paginated.task_skipped_errors.
    """
    client = MagicMock()

    client.submit_transfer.return_value = submit_resp or submit_response()

    if task_responses:
        if len(task_responses) == 1:
            client.get_task.return_value = task_responses[0]
        else:
            client.get_task.side_effect = task_responses

    # paginated.task_skipped_errors(...).items() → list of {"source_path": "..."} dicts
    client.paginated.task_skipped_errors.return_value.items.return_value = [
        {"source_path": p} for p in (skipped_paths or [])
    ]

    return client


# ---------------------------------------------------------------------------
# GlobusTaskCommon (tested via GlobusStatusTask as the simpler concrete subclass)
# ---------------------------------------------------------------------------

class TestGlobusTaskCommon:
    def test_pre_check_passes_all_files_through(self):
        files = [make_file(file_id="a"), make_file(file_id="b")]
        task = GlobusStatusTask("t", files, make_transfer_client(), "task-id")
        to_download, already_done = asyncio.run(task._pre_check(files))
        assert to_download == files
        assert already_done == []

    def test_cleanup_is_passthrough(self):
        from esgpull.downloader.base import TaskResultEvent
        task = GlobusStatusTask("t", [], make_transfer_client(), "task-id")
        result = TaskResultEvent("t", TaskStatus.SUCCESS, "ok", [], {}, None)
        returned = asyncio.run(task._cleanup(result))
        assert returned is result

    def test_to_cancel_returns_started_not_cancelled(self):
        # FileStatus.Started signals the transfer may still be running on Globus;
        # contrast with HttpsDownloadTask.to_cancel() which uses Cancelled.
        files = [make_file(file_id="a"), make_file(file_id="b")]
        task = GlobusStatusTask("t", files, make_transfer_client(), "task-id")
        result = task.to_cancel()
        assert result.status == TaskStatus.CANCELED
        assert all(fr.status == FileStatus.Started for fr in result.files)

    def test_get_extra_contains_task_id_and_status(self):
        task = GlobusStatusTask("t", [], make_transfer_client(), "task-id")
        task._transfer_task_id = "task-id"
        task._globus_task_status = GlobusTransferStatus.ACTIVE
        extra = task._get_extra()
        assert extra["globus_task_id"] == "task-id"
        assert extra["globus_task_status"] == GlobusTransferStatus.ACTIVE


# ---------------------------------------------------------------------------
# GlobusStatusTask — start event suppression
# ---------------------------------------------------------------------------

class TestGlobusStatusTaskStartEvent:
    def test_no_start_event_emitted(self):
        """GlobusStatusTask overrides _emit_start to a no-op; transfers are already in progress."""
        client = make_transfer_client(
            task_responses=[task_response("SUCCEEDED", files_transferred=1)]
        )
        task = GlobusStatusTask("t", [make_file()], client, "task-id",
                                wait_until_resolved=False)
        cb = MagicMock()
        task.on_start(cb)
        asyncio.run(task.run())
        assert cb.call_count == 0


# ---------------------------------------------------------------------------
# GlobusStatusTask — single-check (wait_until_resolved=False)
# ---------------------------------------------------------------------------

class TestGlobusStatusTaskSingleCheck:
    def test_active_status_returns_task_active(self):
        client = make_transfer_client(
            task_responses=[task_response("ACTIVE")]
        )
        task = GlobusStatusTask("t", [make_file()], client, "task-id",
                                wait_until_resolved=False)
        result = asyncio.run(task._run([make_file()], []))
        assert result.status == TaskStatus.ACTIVE
        assert all(fr.status == FileStatus.Started for fr in result.files)

    def test_inactive_status_returns_task_active(self):
        # INACTIVE means the collection is paused; still considered "running"
        client = make_transfer_client(
            task_responses=[task_response("INACTIVE")]
        )
        task = GlobusStatusTask("t", [make_file()], client, "task-id",
                                wait_until_resolved=False)
        result = asyncio.run(task._run([make_file()], []))
        assert result.status == TaskStatus.ACTIVE

    def test_succeeded_status_returns_success(self):
        files = [make_file(file_id="a"), make_file(file_id="b")]
        client = make_transfer_client(
            task_responses=[task_response("SUCCEEDED", files_transferred=2)]
        )
        task = GlobusStatusTask("t", files, client, "task-id",
                                wait_until_resolved=False)
        result = asyncio.run(task._run(files, []))
        assert result.status == TaskStatus.SUCCESS
        assert all(fr.status == FileStatus.Done for fr in result.files)

    def test_failed_status_returns_fail(self):
        client = make_transfer_client(
            task_responses=[task_response("FAILED")]
        )
        task = GlobusStatusTask("t", [make_file()], client, "task-id",
                                wait_until_resolved=False)
        result = asyncio.run(task._run([make_file()], []))
        assert result.status == TaskStatus.FAIL

    def test_heartbeat_emitted_on_each_check(self):
        client = make_transfer_client(
            task_responses=[task_response("ACTIVE", files_transferred=2, bytes_checksummed=512)]
        )
        task = GlobusStatusTask("t", [make_file()], client, "task-id",
                                wait_until_resolved=False)
        beats = []
        task.on_heartbeat(lambda e: beats.append(e))
        asyncio.run(task._run([make_file()], []))
        assert len(beats) == 1
        assert beats[0].n_files_completed == 2
        assert beats[0].bytes_completed == 512


# ---------------------------------------------------------------------------
# GlobusStatusTask — polling (wait_until_resolved=True)
# ---------------------------------------------------------------------------

class TestGlobusStatusTaskPolling:
    def test_polls_until_succeeded(self):
        client = make_transfer_client(task_responses=[
            task_response("ACTIVE"),
            task_response("ACTIVE"),
            task_response("SUCCEEDED", files_transferred=1),
        ])
        task = GlobusStatusTask("t", [make_file()], client, "task-id",
                                wait_until_resolved=True, poll_time_max=0)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = asyncio.run(task._run([make_file()], []))
        assert result.status == TaskStatus.SUCCESS
        assert client.get_task.call_count == 3

    def test_sleep_called_between_polls(self):
        client = make_transfer_client(task_responses=[
            task_response("ACTIVE"),
            task_response("SUCCEEDED"),
        ])
        task = GlobusStatusTask("t", [make_file()], client, "task-id",
                                wait_until_resolved=True)
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            asyncio.run(task._run([make_file()], []))
        # Sleep fires once: after the ACTIVE response, before re-polling
        assert mock_sleep.call_count == 1

    def test_polls_until_failed(self):
        client = make_transfer_client(task_responses=[
            task_response("ACTIVE"),
            task_response("FAILED"),
        ])
        task = GlobusStatusTask("t", [make_file()], client, "task-id",
                                wait_until_resolved=True)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = asyncio.run(task._run([make_file()], []))
        assert result.status == TaskStatus.FAIL


# ---------------------------------------------------------------------------
# GlobusStatusTask — skipped-file handling
# ---------------------------------------------------------------------------

class TestGlobusStatusTaskSkippedFiles:
    def test_skipped_files_get_error_status(self):
        # Requires make_globus_file so that file.globus_fn matches the source_path
        # in the skipped errors response.
        skipped = make_globus_file(file_id="missing", origin_path="/esgf/data")
        ok = make_globus_file(file_id="present", origin_path="/esgf/data")

        # globus_fn = posixpath.join(origin_path, filename)
        skipped_path = skipped.globus_fn

        client = make_transfer_client(
            task_responses=[
                task_response("SUCCEEDED", files_transferred=1,
                              subtasks_skipped_errors=1)
            ],
            skipped_paths=[skipped_path],
        )
        task = GlobusStatusTask("t", [skipped, ok], client, "task-id",
                                wait_until_resolved=False)
        result = asyncio.run(task._run([skipped, ok], []))
        assert result.status == TaskStatus.SUCCESS
        by_id = {fr.file.file_id: fr.status for fr in result.files}
        assert by_id["missing"] == FileStatus.Error
        assert by_id["present"] == FileStatus.Done

    def test_no_skips_all_files_done(self):
        files = [make_globus_file(file_id=f"f{i}") for i in range(3)]
        client = make_transfer_client(
            task_responses=[task_response("SUCCEEDED", files_transferred=3)],
        )
        task = GlobusStatusTask("t", files, client, "task-id",
                                wait_until_resolved=False)
        result = asyncio.run(task._run(files, []))
        assert all(fr.status == FileStatus.Done for fr in result.files)


# ---------------------------------------------------------------------------
# GlobusTransferTask — setup and start event
# ---------------------------------------------------------------------------

class TestGlobusTransferTaskSetup:
    def test_setup_sets_transfer_task_id(self):
        files = [make_globus_file()]
        client = make_transfer_client(submit_resp=submit_response("my-task-uuid"))
        task = GlobusTransferTask(
            "t", files, client,
            source_collection_id="src-uuid",
            dest_collection_id="dst-uuid",
            dest_root_path="/dest",
        )
        asyncio.run(task._setup(files))
        assert task._transfer_task_id == "my-task-uuid"

    def test_task_id_present_in_start_event_extra(self):
        """_setup runs before _emit_start, so the task ID must be in extra when start fires."""
        files = [make_globus_file()]
        client = make_transfer_client(
            submit_resp=submit_response("my-task-uuid"),
            task_responses=[task_response("ACTIVE")],
        )
        task = GlobusTransferTask(
            "t", files, client,
            source_collection_id="src-uuid",
            dest_collection_id="dst-uuid",
            dest_root_path="/dest",
            wait_until_resolved=False,
        )
        start_extras = []
        task.on_start(lambda e: start_extras.append(e.extra))
        asyncio.run(task.run())
        assert start_extras[0]["globus_task_id"] == "my-task-uuid"


# ---------------------------------------------------------------------------
# GlobusTransferTask — run behavior
# ---------------------------------------------------------------------------

class TestGlobusTransferTaskRun:
    def test_no_wait_returns_active_immediately(self):
        files = [make_globus_file()]
        client = make_transfer_client(
            submit_resp=submit_response("task-uuid"),
            task_responses=[task_response("ACTIVE")],
        )
        task = GlobusTransferTask(
            "t", files, client,
            source_collection_id="src-uuid",
            dest_collection_id="dst-uuid",
            dest_root_path="/dest",
            wait_until_resolved=False,
        )
        result = asyncio.run(task.run())
        assert result.status == TaskStatus.ACTIVE
        assert all(fr.status == FileStatus.Started for fr in result.files)

    def test_wait_delegates_to_status_task_and_returns_result(self):
        files = [make_globus_file()]
        client = make_transfer_client(
            submit_resp=submit_response("task-uuid"),
            task_responses=[task_response("SUCCEEDED", files_transferred=1)],
        )
        task = GlobusTransferTask(
            "t", files, client,
            source_collection_id="src-uuid",
            dest_collection_id="dst-uuid",
            dest_root_path="/dest",
            wait_until_resolved=True,
            poll_time=0,
        )
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = asyncio.run(task.run())
        assert result.status == TaskStatus.SUCCESS
        assert all(fr.status == FileStatus.Done for fr in result.files)