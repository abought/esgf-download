"""Unit tests for DownloadTask base class."""
import asyncio
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from esgpull.downloader.base import (
    DownloadTask,
    FileResult,
    TaskHeartbeatEvent,
    TaskResultEvent,
    TaskStartEvent,
    TaskStatus,
)
from esgpull.models import File, FileStatus


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

def make_file(size: int = 0, file_id: str = "file") -> File:
    f = File(
        file_id=file_id,
        dataset_id="dataset",
        master_id="master",
        url=f"https://example.com/{file_id}",
        version="v0",
        filename=f"{file_id}.nc",
        local_path="project/folder",
        data_node="data_node",
        checksum="0",
        checksum_type="0",
        size=size,
        status=FileStatus.Queued,
    )
    f.compute_sha()
    return f


class FakeDownloadTask(DownloadTask):
    """Configurable concrete subclass for testing DownloadTask."""

    def __init__(
        self,
        task_label: str,
        files: list[File],
        *,
        pre_check_result=None,
        run_result=None,
        raise_on_run=None,
        setup_side_effect=None,
    ):
        super().__init__(task_label, files)
        self._pre_check_result = pre_check_result
        self._run_result = run_result
        self._raise_on_run = raise_on_run
        self._setup_side_effect = setup_side_effect

    async def _pre_check(self, files: list[File]) -> tuple[list[File], list[FileResult]]:
        if self._pre_check_result is not None:
            return self._pre_check_result
        return files, []

    async def _setup(self, to_download: list[File]) -> None:
        if self._setup_side_effect:
            self._setup_side_effect(self, to_download)

    async def _run(self, to_download: list[File], skip: list[FileResult]) -> TaskResultEvent:
        if self._raise_on_run is not None:
            raise self._raise_on_run
        if self._run_result is not None:
            return self._run_result
        return self._make_result(TaskStatus.SUCCESS, "ok", [])

    async def _cleanup(self, result: TaskResultEvent) -> TaskResultEvent:
        return result

    def to_cancel(self) -> TaskResultEvent:
        return self._make_result(TaskStatus.CANCELED, "cancelled", [])


class FakeHeartbeatTask(FakeDownloadTask):
    """Variant that emits one heartbeat per run, for callback testing."""

    async def _run(self, to_download, skip):
        self._emit_heartbeat(1, 0)
        return self._make_result(TaskStatus.SUCCESS, "ok", [])


class FakeExtraTask(FakeDownloadTask):
    """Variant that injects a fixed extra dict into all events."""

    def _get_extra(self) -> dict:
        return {"key": "val"}


class FakeExtraHeartbeatTask(FakeExtraTask):
    """FakeExtraTask that also emits a heartbeat, for extra-propagation tests."""

    async def _run(self, to_download, skip):
        self._emit_heartbeat(1, 0)
        return self._make_result(TaskStatus.SUCCESS, "ok", [])


class FakeMutableExtraTask(FakeDownloadTask):
    """Variant whose _get_extra() reads a mutable instance attribute."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.extra_value = "first"

    def _get_extra(self) -> dict:
        return {"value": self.extra_value}


class FakeMutableHeartbeatTask(FakeMutableExtraTask):
    """Mutable-extra variant that emits two heartbeats so mutation is observable."""

    async def _run(self, to_download, skip):
        self._emit_heartbeat(1, 0)
        self.extra_value = "second"
        self._emit_heartbeat(2, 0)
        return self._make_result(TaskStatus.SUCCESS, "ok", [])


# ---------------------------------------------------------------------------
# Callback registration
# ---------------------------------------------------------------------------

class TestCallbackRegistration:
    def test_duplicate_heartbeat_callback_fires_once(self):
        task = FakeHeartbeatTask("t", [make_file()])
        cb = MagicMock()
        task.on_heartbeat(cb)
        task.on_heartbeat(cb)
        asyncio.run(task.run())
        assert cb.call_count == 1

    def test_duplicate_start_callback_fires_once(self):
        task = FakeDownloadTask("t", [make_file()])
        cb = MagicMock()
        task.on_start(cb)
        task.on_start(cb)
        asyncio.run(task.run())
        assert cb.call_count == 1

    def test_duplicate_result_callback_fires_once(self):
        task = FakeDownloadTask("t", [make_file()])
        cb = MagicMock()
        task.on_result(cb)
        task.on_result(cb)
        asyncio.run(task.run())
        assert cb.call_count == 1

    def test_multiple_heartbeat_callbacks_all_fire(self):
        task = FakeHeartbeatTask("t", [make_file()])
        cb1, cb2 = MagicMock(), MagicMock()
        task.on_heartbeat(cb1)
        task.on_heartbeat(cb2)
        asyncio.run(task.run())
        assert cb1.call_count == 1
        assert cb2.call_count == 1


# ---------------------------------------------------------------------------
# run() — event sequence
# ---------------------------------------------------------------------------

class TestRunEventSequence:
    def test_start_fires_before_result(self):
        call_log: list[str] = []
        task = FakeDownloadTask("t", [make_file()])
        task.on_start(lambda e: call_log.append("start"))
        task.on_result(lambda e: call_log.append("result"))
        asyncio.run(task.run())
        assert call_log == ["start", "result"]

    def test_start_fires_when_pre_check_returns_empty_to_download(self):
        f = make_file()
        skip = [FileResult(FileStatus.Done, f)]
        task = FakeDownloadTask("t", [f], pre_check_result=([], skip))
        cb = MagicMock()
        task.on_start(cb)
        asyncio.run(task.run())
        assert cb.call_count == 1
        event: TaskStartEvent = cb.call_args[0][0]
        assert event.files == []
        assert len(event.already_done) == 1

    def test_result_fires_exactly_once(self):
        task = FakeDownloadTask("t", [make_file()])
        cb = MagicMock()
        task.on_result(cb)
        asyncio.run(task.run())
        assert cb.call_count == 1

    def test_heartbeat_not_called_automatically_by_run(self):
        task = FakeDownloadTask("t", [make_file()])
        cb = MagicMock()
        task.on_heartbeat(cb)
        asyncio.run(task.run())
        assert cb.call_count == 0

    def test_heartbeat_fires_when_emitted_in_run(self):
        class HeartbeatTask(FakeDownloadTask):
            async def _run(self, to_download, skip):
                self._emit_heartbeat(1, 512)
                return self._make_result(TaskStatus.SUCCESS, "ok", [])

        task = HeartbeatTask("t", [make_file(size=512)])
        cb = MagicMock()
        task.on_heartbeat(cb)
        asyncio.run(task.run())
        assert cb.call_count == 1
        event: TaskHeartbeatEvent = cb.call_args[0][0]
        assert event.n_files_completed == 1
        assert event.bytes_completed == 512

    def test_multiple_heartbeats_fired_in_order(self):
        class MultiHeartbeatTask(FakeDownloadTask):
            async def _run(self, to_download, skip):
                self._emit_heartbeat(1, 100)
                self._emit_heartbeat(2, 200)
                self._emit_heartbeat(3, 300)
                return self._make_result(TaskStatus.SUCCESS, "ok", [])

        task = MultiHeartbeatTask("t", [make_file(size=300)])
        cb = MagicMock()
        task.on_heartbeat(cb)
        asyncio.run(task.run())
        assert cb.call_count == 3
        completed = [call[0][0].n_files_completed for call in cb.call_args_list]
        assert completed == [1, 2, 3]

    def test_cleanup_modifies_run_result(self):
        class CleanupTask(FakeDownloadTask):
            async def _cleanup(self, result):
                result.msg = "cleaned up"
                return result

        task = CleanupTask("t", [])
        result = asyncio.run(task.run())
        assert result.msg == 'cleaned up'

    def test_start_time_set_before_pre_check(self):
        captured: list = []

        class TimingTask(FakeDownloadTask):
            async def _pre_check(self, files):
                captured.append(self._start_time)
                return files, []

        task = TimingTask("t", [])
        asyncio.run(task.run())
        assert captured[0] is not None
        assert isinstance(captured[0], datetime)


# ---------------------------------------------------------------------------
# TaskStartEvent field correctness
# ---------------------------------------------------------------------------

class TestStartEventFields:
    def _capture_start(self, task: FakeDownloadTask) -> TaskStartEvent:
        captured: list[TaskStartEvent] = []
        task.on_start(lambda e: captured.append(e))
        asyncio.run(task.run())
        return captured[0]

    def test_task_label(self):
        event = self._capture_start(FakeDownloadTask("my-label", [make_file()]))
        assert event.task_label == "my-label"

    def test_files_matches_pre_check_to_download(self):
        f1, f2 = make_file(file_id="a"), make_file(file_id="b")
        task = FakeDownloadTask("t", [f1, f2], pre_check_result=([f1, f2], []))
        event = self._capture_start(task)
        assert event.files == [f1, f2]
        assert event.already_done == []

    def test_already_done_matches_skip_list(self):
        f = make_file()
        skip = [FileResult(FileStatus.Done, f)]
        task = FakeDownloadTask("t", [f], pre_check_result=([], skip))
        event = self._capture_start(task)
        assert len(event.already_done) == 1
        assert event.already_done[0].file is f

    def test_start_time_is_timezone_aware(self):
        event = self._capture_start(FakeDownloadTask("t", []))
        assert isinstance(event.start_time, datetime)
        assert event.start_time.tzinfo is not None


# ---------------------------------------------------------------------------
# TaskHeartbeatEvent field correctness
# ---------------------------------------------------------------------------

class TestHeartbeatEventFields:
    def _capture_heartbeats(self, task: FakeDownloadTask) -> list[TaskHeartbeatEvent]:
        captured: list[TaskHeartbeatEvent] = []
        task.on_heartbeat(lambda e: captured.append(e))
        asyncio.run(task.run())
        return captured

    def test_fields_reflect_emission_and_task_totals(self):
        class SingleHeartbeatTask(FakeDownloadTask):
            async def _run(self, to_download, skip):
                self._emit_heartbeat(3, 512)
                return self._make_result(TaskStatus.SUCCESS, "ok", [])

        files = [make_file(size=100, file_id="a"), make_file(size=200, file_id="b")]
        task = SingleHeartbeatTask("hb-label", files)
        events = self._capture_heartbeats(task)
        event = events[0]
        assert event.task_label == "hb-label"
        assert event.n_files_completed == 3
        assert event.bytes_completed == 512
        assert event.n_files_expected == 2
        assert event.bytes_expected == 300

    def test_event_time_is_timezone_aware(self):
        task = FakeHeartbeatTask("t", [make_file()])
        events = self._capture_heartbeats(task)
        assert events[0].event_time.tzinfo is not None


# ---------------------------------------------------------------------------
# TaskResultEvent field correctness
# ---------------------------------------------------------------------------

class TestResultEventFields:
    def test_task_label(self):
        task = FakeDownloadTask("my-label", [])
        result = asyncio.run(task.run())
        assert result.task_label == "my-label"

    def test_start_time_matches_task_start_time(self):
        task = FakeDownloadTask("t", [])
        result = asyncio.run(task.run())
        assert result.start_time == task._start_time

    def test_end_time_is_not_before_start(self):
        task = FakeDownloadTask("t", [])
        result = asyncio.run(task.run())
        assert result.end_time >= result.start_time


# ---------------------------------------------------------------------------
# .extra propagation
# ---------------------------------------------------------------------------

class TestExtraPropagation:
    def test_extra_in_start_event(self):
        task = FakeExtraTask("t", [])
        captured: list[TaskStartEvent] = []
        task.on_start(lambda e: captured.append(e))
        asyncio.run(task.run())
        assert captured[0].extra == {"key": "val"}

    def test_extra_in_heartbeat_event(self):
        task = FakeExtraHeartbeatTask("t", [make_file()])
        captured: list[TaskHeartbeatEvent] = []
        task.on_heartbeat(lambda e: captured.append(e))
        asyncio.run(task.run())
        assert captured[0].extra == {"key": "val"}

    def test_extra_in_result_event(self):
        task = FakeExtraTask("t", [])
        captured: list[TaskResultEvent] = []
        task.on_result(lambda e: captured.append(e))
        asyncio.run(task.run())
        assert captured[0].extra == {"key": "val"}

    def test_extra_reevaluated_per_emission(self):
        task = FakeMutableHeartbeatTask("t", [make_file(file_id="a")])
        captured: list[dict] = []
        task.on_heartbeat(lambda e: captured.append(e.extra.copy()))
        asyncio.run(task.run())
        assert captured[0] == {"value": "first"}
        assert captured[1] == {"value": "second"}


# ---------------------------------------------------------------------------
# to_fail() behavior
# ---------------------------------------------------------------------------

class TestToFail:
    def test_status_is_fail(self):
        assert FakeDownloadTask("t", []).to_fail().status == TaskStatus.FAIL

    def test_before_run_reports_all_constructor_files(self):
        f1, f2 = make_file(file_id="a"), make_file(file_id="b")
        task = FakeDownloadTask("t", [f1, f2])
        result_files = [fr.file for fr in task.to_fail().files]
        assert f1 in result_files
        assert f2 in result_files

    def test_during_run_reports_only_in_flight_files(self):
        """to_fail() after _pre_check should report only files being downloaded."""
        f_inflight = make_file(file_id="a")
        f_done = make_file(file_id="b")
        captured: list[TaskResultEvent] = []

        class CapturingTask(FakeDownloadTask):
            async def _run(self, to_download, skip):
                captured.append(self.to_fail())
                return self._make_result(TaskStatus.SUCCESS, "ok", [])

        skip = [FileResult(FileStatus.Done, f_done)]
        task = CapturingTask("t", [f_inflight, f_done], pre_check_result=([f_inflight], skip))
        asyncio.run(task.run())
        fail_files = [fr.file for fr in captured[0].files]
        assert f_inflight in fail_files
        assert f_done not in fail_files

    def test_custom_message(self):
        assert FakeDownloadTask("t", []).to_fail("disk full").msg == "disk full"

    def test_default_message(self):
        assert FakeDownloadTask("t", []).to_fail().msg == "An unknown error occurred"

    def test_start_time_none_before_run(self):
        assert FakeDownloadTask("t", []).to_fail().start_time is None

    def test_start_time_set_after_run_starts(self):
        task = FakeDownloadTask("t", [])
        task._start_time = datetime.now()
        assert task.to_fail().start_time == task._start_time

    def test_file_results_have_error_status(self):
        task = FakeDownloadTask("t", [make_file()])
        assert all(fr.status == FileStatus.Error for fr in task.to_fail().files)


# ---------------------------------------------------------------------------
# Cancellation and error paths
# ---------------------------------------------------------------------------

class TestCancellationAndErrors:
    def test_cancelled_error_is_reraised(self):
        task = FakeDownloadTask("t", [make_file()], raise_on_run=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(task.run())

    def test_cancelled_error_runs_cleanup_with_cancel_result(self):
        cleanup_statuses: list[TaskStatus] = []

        class TrackingTask(FakeDownloadTask):
            async def _cleanup(self, result):
                cleanup_statuses.append(result.status)
                return result

        task = TrackingTask("t", [make_file()], raise_on_run=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(task.run())
        assert cleanup_statuses == [TaskStatus.CANCELED]

    def test_cancelled_error_does_not_emit_result(self):
        # This scenario is handled by the pipeline, which adds extra error capturing capabilities
        task = FakeDownloadTask("t", [make_file()], raise_on_run=asyncio.CancelledError())
        cb = MagicMock()
        task.on_result(cb)
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(task.run())
        assert cb.call_count == 0

    def test_keyboard_interrupt_is_reraised(self):
        task = FakeDownloadTask("t", [make_file()], raise_on_run=KeyboardInterrupt())
        with pytest.raises(KeyboardInterrupt):
            asyncio.run(task.run())

    def test_keyboard_interrupt_runs_cleanup_with_cancel_result(self):
        cleanup_statuses: list[TaskStatus] = []

        class TrackingTask(FakeDownloadTask):
            async def _cleanup(self, result):
                cleanup_statuses.append(result.status)
                return result

        task = TrackingTask("t", [make_file()], raise_on_run=KeyboardInterrupt())
        with pytest.raises(KeyboardInterrupt):
            asyncio.run(task.run())
        assert cleanup_statuses == [TaskStatus.CANCELED]

    def test_start_fires_before_cancellation(self):
        task = FakeDownloadTask("t", [make_file()], raise_on_run=asyncio.CancelledError())
        cb = MagicMock()
        task.on_start(cb)
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(task.run())
        assert cb.call_count == 1

    def test_start_time_set_even_when_run_raises(self):
        task = FakeDownloadTask("t", [make_file()], raise_on_run=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(task.run())
        assert task._start_time is not None


# ---------------------------------------------------------------------------
# _setup() ordering
# ---------------------------------------------------------------------------

class TestSetupOrdering:
    def test_setup_completes_before_start_fires(self):
        setup_done: list[bool] = []
        start_saw_setup: list[bool] = []

        def side_effect(task_self, to_download):
            setup_done.append(True)

        task = FakeDownloadTask("t", [make_file()], setup_side_effect=side_effect)
        task.on_start(lambda e: start_saw_setup.append(bool(setup_done)))
        asyncio.run(task.run())
        assert start_saw_setup == [True]

    def test_default_setup_does_not_raise(self):
        task = FakeDownloadTask("t", [make_file()])
        asyncio.run(task.run())
