import asyncio
import errno
import os
import shutil
import types

import pytest

from esgpull import Esgpull
from esgpull.database import Database
from esgpull.downloader.as_https import check_disk_space
from esgpull.downloader.base import (
    FileResult,
    TaskHeartbeatEvent,
    TaskResultEvent,
    TaskStartEvent,
    TaskStatus,
)
from esgpull.downloader.factory import add_https_tasks
from esgpull.downloader.fs import Filesystem
from esgpull.downloader.orchestrator import Orchestrator
from esgpull.downloader.ui import HttpsDownloadUI
from esgpull.downloader.callbacks import (
    is_disk_full as _is_disk_full,
    make_on_task_start as _make_on_task_start,
    process_task_result as _process_task_result,
)
from esgpull.exceptions import InsufficientDiskSpace
from esgpull.models import File, FileStatus
from esgpull.tui import UI
from tests.downloader.fakes import FakeDownloadTask, make_file


pytestmark = pytest.mark.skip(reason="These tests are under review and require significant changes to be useful")


@pytest.fixture
def fs(config) -> Filesystem:
    return Filesystem.from_config(config, install=True)


def persist(db: Database, file: File) -> File:
    db.add(file)
    return file


class FakeApp:
    """Duck-types the bits of `Esgpull` that `factory.make_https_tasks` reads."""

    def __init__(self, config, fs: Filesystem) -> None:
        self.config = config
        self.fs = fs


class FakeCancelTask(FakeDownloadTask):
    """Unlike `FakeDownloadTask.to_cancel()` (which reports an empty file
    list), mirrors `HttpsDownloadTask.to_cancel()`'s real behavior of
    reporting one `FileResult` per bundled file."""

    def to_cancel(self) -> TaskResultEvent:
        files = self._to_download or self._files
        return self._make_result(
            TaskStatus.CANCELED,
            "cancelled",
            [FileResult(FileStatus.Cancelled, f) for f in files],
        )


class TestProcessEvent:
    def test_done_file_has_no_errors(self, db):
        file = persist(db, make_file())
        event = TaskResultEvent(
            "t", TaskStatus.COMPLETE, "ok", [FileResult(FileStatus.Done, file)], {}, None
        )
        errors = _process_task_result(db, event, use_db=True)
        assert errors == []
        assert file.status == FileStatus.Done

    def test_per_file_error_inside_a_successful_task_is_still_an_error(self, db):
        """Regression test: `TaskStatus.COMPLETE` at the task level does not
        mean every bundled file succeeded -- `HttpsDownloadTask._run()` can
        return COMPLETE even when one file's download failed inside its loop;
        only `FileResult.status` reflects the per-file outcome."""
        file = persist(db, make_file())
        event = TaskResultEvent(
            "t",
            TaskStatus.COMPLETE,
            "Download complete",
            [FileResult(FileStatus.Error, file)],
            {},
            None,
        )
        errors = _process_task_result(db, event, use_db=True)
        assert len(errors) == 1
        assert errors[0].data is file
        assert file.status == FileStatus.Error

    def test_cancelled_file(self, db):
        file = persist(db, make_file())
        event = TaskResultEvent(
            "t", TaskStatus.CANCELED, "cancelled", [FileResult(FileStatus.Cancelled, file)], {}, None
        )
        errors = _process_task_result(db, event, use_db=True)
        assert errors == []
        assert file.status == FileStatus.Cancelled

    def test_task_level_fail_marks_all_bundled_files_as_error(self, db):
        f1 = persist(db, make_file(file_id="f1"))
        f2 = persist(db, make_file(file_id="f2"))
        event = TaskResultEvent(
            "t",
            TaskStatus.FAIL,
            "ENOSPC",
            [FileResult(FileStatus.Error, f1), FileResult(FileStatus.Error, f2)],
            {},
            None,
        )
        errors = _process_task_result(db, event, use_db=True)
        assert len(errors) == 2
        assert f1.status == FileStatus.Error
        assert f2.status == FileStatus.Error

    def test_unexpected_file_status_raises(self, db):
        """Non-terminal statuses (e.g. Started) in a completed task result are a
        bug or data corruption — should halt downloads immediately, not silently
        queue an error."""
        file = persist(db, make_file())
        event = TaskResultEvent(
            "t", TaskStatus.COMPLETE, "ok", [FileResult(FileStatus.Started, file)], {}, None
        )
        with pytest.raises(RuntimeError, match="Unexpected file status"):
            _process_task_result(db, event, use_db=True)


class TestMakeOnTaskStart:
    def test_starting_files_are_marked_starting(self, db):
        file = persist(db, make_file())
        on_start = _make_on_task_start(db, use_db=True)
        event = TaskStartEvent("t", [file], [], {})
        on_start(event)
        assert file.status == FileStatus.Starting

    def test_already_done_files_are_marked_done(self, db):
        file = persist(db, make_file())
        on_start = _make_on_task_start(db, use_db=True)
        event = TaskStartEvent("t", [], [FileResult(FileStatus.Done, file)], {})
        on_start(event)
        assert file.status == FileStatus.Done

    def test_use_db_false_skips_db_write(self, db, monkeypatch):
        file = persist(db, make_file())
        add_calls: list = []
        monkeypatch.setattr(db, "add", lambda *a, **kw: add_calls.append(a))
        on_start = _make_on_task_start(db, use_db=False)
        event = TaskStartEvent("t", [file], [], {})
        on_start(event)
        assert file.status == FileStatus.Starting
        assert add_calls == []


class TestDrainCancels:
    def test_queued_but_unstarted_tasks_are_marked_cancelled(self, db):
        f1 = persist(db, make_file(file_id="c1"))
        f2 = persist(db, make_file(file_id="c2"))

        async def run():
            orch = Orchestrator()
            orch.add_local_task(FakeCancelTask("t1", [f1]))
            orch.add_local_task(FakeCancelTask("t2", [f2]))
            # Never call iter_results(): tasks remain queued, unstarted.
            return await orch.collect_cancels()

        errors = asyncio.run(run())
        assert errors == []
        assert f1.status == FileStatus.Cancelled
        assert f2.status == FileStatus.Cancelled


class TestIsDiskFull:
    def test_true_when_fail_and_enospc_in_message(self):
        event = TaskResultEvent("t", TaskStatus.FAIL, os.strerror(errno.ENOSPC), [], {}, None)
        assert _is_disk_full(event)

    def test_false_when_fail_but_not_enospc(self):
        event = TaskResultEvent("t", TaskStatus.FAIL, "some other failure", [], {}, None)
        assert not _is_disk_full(event)

    def test_false_when_task_succeeded(self):
        event = TaskResultEvent("t", TaskStatus.COMPLETE, os.strerror(errno.ENOSPC), [], {}, None)
        assert not _is_disk_full(event)


class TestCheckDiskSpace:
    def test_raises_when_insufficient(self, fs, monkeypatch):
        file = make_file(size=10**12)
        monkeypatch.setattr(shutil, "disk_usage", lambda path: types.SimpleNamespace(free=0))
        with pytest.raises(InsufficientDiskSpace):
            check_disk_space([file], fs)

    def test_passes_when_sufficient(self, fs, monkeypatch):
        file = make_file(size=10)
        monkeypatch.setattr(
            shutil, "disk_usage", lambda path: types.SimpleNamespace(free=10**12)
        )
        check_disk_space([file], fs)


class TestHttpsDownloadUI:
    def test_on_start_marks_already_done_progress(self, config):
        file = make_file(size=100)
        ui = HttpsDownloadUI(UI.from_config(config), 1, show_filename=False, show_progress=False)
        ui.on_start(TaskStartEvent("t", [], [FileResult(FileStatus.Done, file)], {}))
        assert ui.main_progress.tasks[0].completed == 1

    def test_on_heartbeat_updates_file_progress(self, config):
        file = make_file(size=100)
        ui = HttpsDownloadUI(UI.from_config(config), 1, show_filename=False, show_progress=False)
        ui.on_start(TaskStartEvent("t", [file], [], {}))
        ui.on_heartbeat(TaskHeartbeatEvent(file.file_id, 0, 1, 42, 100, {}))
        task_id = ui._task_ids[file.file_id]
        idx = ui.file_progress.task_ids.index(task_id)
        assert ui.file_progress.tasks[idx].completed == 42

    def test_on_result_success_advances_main_progress(self, config):
        file = make_file(size=10)
        ui = HttpsDownloadUI(UI.from_config(config), 1, show_filename=False, show_progress=False)
        ui.on_start(TaskStartEvent(file.file_id, [file], [], {}))
        event = TaskResultEvent(
            file.file_id, TaskStatus.COMPLETE, "ok", [FileResult(FileStatus.Done, file)], {}, None
        )
        ui.on_result(event)
        assert ui.main_progress.tasks[0].completed == 1
        assert ui.file_progress.tasks == []

    def test_on_result_per_file_error_increments_error_count(self, config):
        """Regression test for the TaskStatus/FileStatus bug: a SUCCESS-status
        task whose single bundled file failed must still count as an error in
        the progress UI, not as a completed download."""
        file = make_file(size=10)
        ui = HttpsDownloadUI(UI.from_config(config), 1, show_filename=False, show_progress=False)
        ui.on_start(TaskStartEvent(file.file_id, [file], [], {}))
        event = TaskResultEvent(
            file.file_id,
            TaskStatus.COMPLETE,
            "Download complete",
            [FileResult(FileStatus.Error, file)],
            {},
            None,
        )
        ui.on_result(event)
        assert ui._nb_errors == 1
        assert ui.main_progress.tasks[0].completed == 0


class TestAddHttpsTasks:
    def test_already_done_file_skips_network_and_writes_db(self, config, fs, db):
        async def run():
            file = persist(db, make_file(size=0))
            fs[file].drs.parent.mkdir(parents=True, exist_ok=True)
            fs[file].drs.touch()

            ui = HttpsDownloadUI(UI.from_config(config), 1, show_filename=False, show_progress=False)
            orch = Orchestrator()
            add_https_tasks(orch, [file], FakeApp(config, fs), ui)
            orch.on_task_start(_make_on_task_start(db, use_db=True))
            return [event async for event in orch.iter_results()], file

        results, file = asyncio.run(run())
        assert len(results) == 1
        assert results[0].files == []  # the file was already_done, not in .files
        assert file.status == FileStatus.Done


class TestDownload2HttpsPreflight:
    def test_insufficient_disk_space_raises_before_any_task_runs(self, root, monkeypatch):
        esg = Esgpull(root, install=True)
        file = make_file(size=10**15)
        monkeypatch.setattr(shutil, "disk_usage", lambda path: types.SimpleNamespace(free=0))
        with pytest.raises(InsufficientDiskSpace):
            asyncio.run(esg.download2_https([file], show_progress=False))


class TestDownload2HttpsEndToEnd:
    def test_single_failing_url_marks_file_error_and_returns_in_errors(self, root):
        esg = Esgpull(root, install=True)
        file = File(
            file_id="failing_file",
            dataset_id="test_dataset_id",
            master_id="test_master_id",
            url="https://nonexistent.path/to/file.nc",
            version="1.0",
            filename="failing.nc",
            local_path="test/failing",
            data_node="test_node",
            checksum="12345",
            checksum_type="SHA256",
            size=1000,
            status=FileStatus.Queued,
        )
        file.compute_sha()
        esg.db.add(file)

        files, errors = asyncio.run(esg.download2_https([file], show_progress=False))

        assert files == []
        assert len(errors) == 1
        assert errors[0].data.file_id == "failing_file"
        assert file.status == FileStatus.Error
