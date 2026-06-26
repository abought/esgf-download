"""Unit tests for HttpsDownloadTask."""
import asyncio
import hashlib

import httpx
import pytest

from esgpull.downloader.as_https import HttpsDownloadTask
from esgpull.downloader.base import FileResult, TaskResultEvent, TaskStatus
from esgpull.downloader.fs import FilePath
from esgpull.models import File, FileStatus
from tests.downloader.fakes import make_file


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

def make_https_file(content: bytes, file_id: str = "file") -> File:
    """Create a File whose checksum matches `content` exactly.

    Digest requires checksum_type="SHA256" (uppercase); make_file() uses "0"
    and cannot be used for any test that exercises checksum validation.
    """
    f = File(
        file_id=file_id,
        dataset_id="dataset",
        master_id="master",
        url=f"https://example.com/{file_id}.nc",
        version="v0",
        filename=f"{file_id}.nc",
        local_path="project/folder",
        data_node="data_node",
        checksum=hashlib.sha256(content).hexdigest(),
        checksum_type="SHA256",
        size=len(content),
        status=FileStatus.Queued,
    )
    f.compute_sha()
    return f


class FakeFilesystem:
    """Minimal Filesystem stand-in backed by a real pytest tmp_path directory."""

    def __init__(self, base):
        self._base = base
        (base / "tmp").mkdir(parents=True, exist_ok=True)

    def __getitem__(self, file: File) -> FilePath:
        return FilePath(
            drs=self._base / "drs" / file.local_path / file.filename,
            tmp=self._base / "tmp" / f"{file.sha}.part",
        )

    def move_to_drs(self, file: File) -> None:
        fp = self[file]
        fp.drs.parent.mkdir(parents=True, exist_ok=True)
        fp.done.rename(fp.drs)


def make_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def success_client(content: bytes) -> httpx.AsyncClient:
    return make_client(lambda req: httpx.Response(200, content=content))


@pytest.fixture(scope="function")
def fs(tmp_path):
    return FakeFilesystem(tmp_path)


class TestPreCheck:
    def test_absent_file_goes_to_download(self, fs):
        file = make_file()
        task = HttpsDownloadTask("t", [file], fs=fs)
        to_download, already_done = asyncio.run(task._pre_check([file]))
        assert file in to_download
        assert already_done == []

    def test_present_file_goes_to_already_done(self, fs):
        file = make_file()
        fp = fs[file]
        fp.drs.parent.mkdir(parents=True, exist_ok=True)
        fp.drs.write_bytes(b"existing")
        task = HttpsDownloadTask("t", [file], fs=fs)
        to_download, already_done = asyncio.run(task._pre_check([file]))
        assert to_download == []
        assert len(already_done) == 1
        assert already_done[0].status == FileStatus.Done
        assert already_done[0].file is file

    def test_mixed_files_split_correctly(self, fs):
        present = make_file(file_id="present")
        absent = make_file(file_id="absent")
        fp = fs[present]
        fp.drs.parent.mkdir(parents=True, exist_ok=True)
        fp.drs.write_bytes(b"existing")
        task = HttpsDownloadTask("t", [present, absent], fs=fs)
        to_download, already_done = asyncio.run(task._pre_check([present, absent]))
        assert absent in to_download
        assert present not in to_download
        assert any(fr.file is present for fr in already_done)

    def test_empty_input_returns_empty_lists(self, fs):
        task = HttpsDownloadTask("t", [], fs=fs)
        to_download, already_done = asyncio.run(task._pre_check([]))
        assert to_download == []
        assert already_done == []


class TestToCancel:
    def test_returns_canceled_status(self, fs):
        task = HttpsDownloadTask("t", [make_file()], fs=fs)
        assert task.to_cancel().status == TaskStatus.CANCELED

    def test_files_get_cancelled_not_error(self, fs):
        # FileStatus.Cancelled (not Error) keeps files eligible for retry
        f1, f2 = make_file(file_id="a"), make_file(file_id="b")
        task = HttpsDownloadTask("t", [f1, f2], fs=fs)
        result = task.to_cancel()
        assert all(fr.status == FileStatus.Cancelled for fr in result.files)


class TestRunHappyPath:
    def test_success_response_yields_done_result(self, fs):
        content = b"hello world"
        file = make_https_file(content)
        task = HttpsDownloadTask("t", [file], fs=fs, client=success_client(content))
        result = asyncio.run(task.run())
        assert result.files[0].status == FileStatus.Done
        assert result.files[0].file is file

    def test_task_result_is_always_success(self, fs):
        """run() always returns TaskStatus.COMPLETE; per-file errors live in result.files."""
        file = make_file()
        task = HttpsDownloadTask(
            "t", [file], fs=fs,
            client=make_client(lambda req: httpx.Response(404)),
            disable_checksum=True,
        )
        result = asyncio.run(task.run())
        assert result.status == TaskStatus.COMPLETE

    def test_part_file_absent_after_successful_run(self, fs):
        content = b"data"
        file = make_https_file(content)
        task = HttpsDownloadTask("t", [file], fs=fs, client=success_client(content))
        asyncio.run(task.run())
        assert not fs[file].tmp.is_file()

    def test_done_file_renamed_before_cleanup(self, fs):
        # Tests mid-lifecycle state: _run renames .part → .done before _cleanup
        # moves it to DRS. Calls _run directly because run() completes cleanup.
        content = b"data"
        file = make_https_file(content)
        task = HttpsDownloadTask("t", [file], fs=fs, client=success_client(content))
        asyncio.run(task._run([file], []))
        assert fs[file].done.is_file()

    def test_multiple_files_all_succeed(self, fs):
        files = [make_https_file(b"content", file_id=f"f{i}") for i in range(3)]
        task = HttpsDownloadTask(
            "t", files, fs=fs,
            client=make_client(lambda req: httpx.Response(200, content=b"content")),
        )
        result = asyncio.run(task.run())
        assert len(result.files) == 3
        assert all(fr.status == FileStatus.Done for fr in result.files)


class TestRunHeartbeatRhythm:
    def test_one_heartbeat_per_chunk(self, fs):
        content = b"x" * 30
        file = make_https_file(content)
        task = HttpsDownloadTask(
            "t", [file], fs=fs,
            client=success_client(content),
            chunk_size=10,
        )
        beats = []
        task.on_heartbeat(lambda e: beats.append(e))
        asyncio.run(task._run([file], []))
        # 3 chunks emit heartbeats with files_completed=0 (before file is counted done)
        chunk_beats = [b for b in beats if b.n_files_completed == 0]
        assert len(chunk_beats) == 3

    def test_one_additional_heartbeat_per_file_completion(self, fs):
        content = b"data"
        file = make_https_file(content)
        task = HttpsDownloadTask("t", [file], fs=fs, client=success_client(content))
        beats = []
        task.on_heartbeat(lambda e: beats.append(e.n_files_completed))
        asyncio.run(task._run([file], []))
        # Final heartbeat fires after files_completed is incremented to 1
        assert beats[-1] == 1

    def test_files_completed_starts_at_len_skip(self, fs):
        content = b"data"
        file = make_https_file(content)
        skip = [FileResult(FileStatus.Done, make_file(file_id="already-done"))]
        task = HttpsDownloadTask("t", [file], fs=fs, client=success_client(content))
        beats = []
        task.on_heartbeat(lambda e: beats.append(e.n_files_completed))
        asyncio.run(task._run([file], skip))
        assert beats[0] == len(skip)

    def test_streaming_heartbeats_reflect_incremental_progress(self, fs):
        content = b"x" * 30
        file = make_https_file(content)
        task = HttpsDownloadTask(
            "t", [file], fs=fs,
            client=success_client(content),
            chunk_size=10,
        )
        beats = []
        task.on_heartbeat(lambda e: beats.append(e))
        asyncio.run(task.run())
        chunk_beats = [b for b in beats if b.n_files_completed == 0]
        assert [b.bytes_completed for b in chunk_beats] == [10, 20, 30]

    def test_bytes_completed_is_cumulative(self, fs):
        content = b"x" * 10
        f1 = make_https_file(content, file_id="a")
        f2 = make_https_file(content, file_id="b")
        task = HttpsDownloadTask(
            "t", [f1, f2], fs=fs,
            client=make_client(lambda req: httpx.Response(200, content=content)),
        )
        beats = []
        task.on_heartbeat(lambda e: beats.append(e.bytes_completed))
        asyncio.run(task._run([f1, f2], []))
        assert beats == sorted(beats)   # monotonically non-decreasing
        assert beats[-1] == 20          # both files' bytes accounted for


class TestRunChecksumValidation:
    def test_correct_checksum_yields_done(self, fs):
        content = b"correct content"
        file = make_https_file(content)
        task = HttpsDownloadTask("t", [file], fs=fs, client=success_client(content))
        result = asyncio.run(task.run())
        assert result.files[0].status == FileStatus.Done

    def test_wrong_checksum_yields_error_and_cleans_up(self, fs):
        content = b"actual content"
        file = make_https_file(content)
        file.checksum = "0" * 64  # tamper after construction
        task = HttpsDownloadTask("t", [file], fs=fs, client=success_client(content))
        result = asyncio.run(task.run())
        assert result.files[0].status == FileStatus.Error
        assert not fs[file].drs.is_file()
        assert not fs[file].done.is_file()
        assert not fs[file].tmp.is_file()

    def test_checksum_mismatch_leaves_done_file_for_cleanup(self, fs):
        # Tests the _run → _cleanup contract: _run renames .part → .done before
        # validating, so cleanup has a file to unlink on mismatch.
        content = b"actual content"
        file = make_https_file(content)
        file.checksum = "0" * 64
        task = HttpsDownloadTask("t", [file], fs=fs, client=success_client(content))
        asyncio.run(task._run([file], []))
        assert fs[file].done.is_file()

    def test_disable_checksum_yields_done_regardless(self, fs):
        content = b"anything"
        file = make_https_file(content)
        file.checksum = "0" * 64  # wrong, but ignored
        task = HttpsDownloadTask(
            "t", [file], fs=fs,
            client=success_client(content),
            disable_checksum=True,
        )
        result = asyncio.run(task.run())
        assert result.files[0].status == FileStatus.Done


class TestRunHttpErrors:
    # disable_checksum=True + make_file(): Digest is constructed before the HTTP
    # request, so checksum_type="0" (from make_file) would raise NotImplementedError
    # before any network call. disable_checksum avoids creating Digest at all.
    def _task(self, fs, handler):
        file = make_file()
        task = HttpsDownloadTask(
            "t", [file], fs=fs,
            client=make_client(handler),
            disable_checksum=True,
        )
        return task, file

    def test_404_yields_error_and_cleans_up(self, fs):
        task, file = self._task(fs, lambda req: httpx.Response(404))
        result = asyncio.run(task.run())
        assert result.files[0].status == FileStatus.Error
        assert not fs[file].drs.is_file()
        assert not fs[file].tmp.is_file()

    def test_500_yields_error_and_cleans_up(self, fs):
        task, file = self._task(fs, lambda req: httpx.Response(500))
        result = asyncio.run(task.run())
        assert result.files[0].status == FileStatus.Error
        assert not fs[file].drs.is_file()
        assert not fs[file].tmp.is_file()

    def test_connect_error_yields_error_and_cleans_up(self, fs):
        def handler(req):
            raise httpx.ConnectError("connection refused")
        task, file = self._task(fs, handler)
        result = asyncio.run(task.run())
        assert result.files[0].status == FileStatus.Error
        assert not fs[file].drs.is_file()
        assert not fs[file].tmp.is_file()

    def test_timeout_yields_error_and_cleans_up(self, fs):
        def handler(req):
            raise httpx.TimeoutException("timed out")
        task, file = self._task(fs, handler)
        result = asyncio.run(task.run())
        assert result.files[0].status == FileStatus.Error
        assert not fs[file].drs.is_file()
        assert not fs[file].tmp.is_file()

    def test_failed_file_does_not_stop_subsequent_files(self, fs):
        # NOTE: HttpsDownloadTask supports multiple files in one task via a bare
        # except: that continues the per-file loop. In practice each HTTPS file
        # should be its own task so the orchestrator can parallelise downloads;
        # multi-file tasks are technically supported but discouraged.
        content = b"ok"
        ok_file = make_https_file(content, file_id="ok")
        bad_file = make_file(file_id="bad")

        def handler(req):
            if "bad" in str(req.url):
                return httpx.Response(404)
            return httpx.Response(200, content=content)

        task = HttpsDownloadTask(
            "t", [bad_file, ok_file], fs=fs,
            client=make_client(handler),
            disable_checksum=True,
        )
        result = asyncio.run(task.run())
        by_id = {fr.file.file_id: fr.status for fr in result.files}
        assert by_id["bad"] == FileStatus.Error
        assert by_id["ok"] == FileStatus.Done
        assert not fs[bad_file].tmp.is_file()
        assert fs[ok_file].drs.is_file()


class TestRunClientLifecycle:
    def test_provided_client_not_closed_after_run(self, fs):
        content = b"data"
        file = make_https_file(content)
        client = success_client(content)
        task = HttpsDownloadTask("t", [file], fs=fs, client=client)
        asyncio.run(task.run())
        assert not client.is_closed


class TestCleanup:
    def _task(self, fs):
        return HttpsDownloadTask("t", [], fs=fs)

    def _result(self, file, status):
        return TaskResultEvent("t", TaskStatus.COMPLETE, "ok", [FileResult(status, file)], {}, None)

    def test_done_file_moved_to_drs(self, fs):
        content = b"data"
        file = make_https_file(content)
        fp = fs[file]
        fp.done.parent.mkdir(parents=True, exist_ok=True)
        fp.done.write_bytes(content)
        asyncio.run(self._task(fs)._cleanup(self._result(file, FileStatus.Done)))
        assert fp.drs.is_file()
        assert not fp.done.is_file()

    def test_done_absent_for_success_file_no_error(self, fs):
        file = make_https_file(b"data")
        asyncio.run(self._task(fs)._cleanup(self._result(file, FileStatus.Done)))

    def test_error_file_done_path_unlinked(self, fs):
        file = make_file()
        fp = fs[file]
        fp.done.parent.mkdir(parents=True, exist_ok=True)
        fp.done.write_bytes(b"partial")
        asyncio.run(self._task(fs)._cleanup(self._result(file, FileStatus.Error)))
        assert not fp.done.is_file()

    def test_error_file_tmp_path_unlinked(self, fs):
        file = make_file()
        fp = fs[file]
        fp.tmp.parent.mkdir(parents=True, exist_ok=True)
        fp.tmp.write_bytes(b"partial")
        asyncio.run(self._task(fs)._cleanup(self._result(file, FileStatus.Error)))
        assert not fp.tmp.is_file()

    def test_error_file_no_temp_files_no_error(self, fs):
        file = make_file()
        asyncio.run(self._task(fs)._cleanup(self._result(file, FileStatus.Error)))

    def test_returns_same_result_object(self, fs):
        result = TaskResultEvent("t", TaskStatus.COMPLETE, "ok", [], {}, None)
        returned = asyncio.run(self._task(fs)._cleanup(result))
        assert returned is result

    def test_move_to_drs_error_yields_task_fail(self, fs):
        file = make_https_file(b"data")
        fp = fs[file]
        fp.done.parent.mkdir(parents=True, exist_ok=True)
        fp.done.write_bytes(b"data")

        def bad_move(f):
            raise OSError("permission denied")
        fs.move_to_drs = bad_move

        task = HttpsDownloadTask("t", [file], fs=fs)
        task._to_download = [file]
        result = asyncio.run(task._cleanup(self._result(file, FileStatus.Done)))
        assert result.status == TaskStatus.FAIL
        assert all(fr.status == FileStatus.Error for fr in result.files)


class TestRunTaskLevelErrors:
    def test_disk_full_yields_task_fail(self, fs, monkeypatch):
        import errno as errno_module
        from esgpull.downloader import as_https

        def bad_open(*args, **kwargs):
            raise OSError(errno_module.ENOSPC, "No space left on device")

        monkeypatch.setattr(as_https.aiofiles, "open", bad_open)

        file = make_file()
        task = HttpsDownloadTask(
            "t", [file], fs=fs,
            client=make_client(lambda req: httpx.Response(200, content=b"data")),
            disable_checksum=True,
        )
        result = asyncio.run(task.run())
        assert result.status == TaskStatus.FAIL
        assert all(fr.status == FileStatus.Error for fr in result.files)

    def test_disk_full_stops_remaining_files(self, fs, monkeypatch):
        """ENOSPC on the first file should not attempt the second."""
        import errno as errno_module
        from esgpull.downloader import as_https

        open_calls = []

        def bad_open(*args, **kwargs):
            open_calls.append(args[0])
            raise OSError(errno_module.ENOSPC, "No space left on device")

        monkeypatch.setattr(as_https.aiofiles, "open", bad_open)

        f1, f2 = make_file(file_id="a"), make_file(file_id="b")
        task = HttpsDownloadTask(
            "t", [f1, f2], fs=fs,
            client=make_client(lambda req: httpx.Response(200, content=b"data")),
            disable_checksum=True,
        )
        result = asyncio.run(task.run())
        assert result.status == TaskStatus.FAIL
        assert len(open_calls) == 1  # stopped after first failure


class TestIntegration:
    def test_happy_path_file_reaches_drs(self, fs):
        content = b"final content"
        file = make_https_file(content)
        asyncio.run(
            HttpsDownloadTask("t", [file], fs=fs, client=success_client(content)).run()
        )
        assert fs[file].drs.is_file()
        assert fs[file].drs.read_bytes() == content

    def test_already_done_file_skipped(self, fs):
        content = b"existing"
        file = make_https_file(content)
        fp = fs[file]
        fp.drs.parent.mkdir(parents=True, exist_ok=True)
        fp.drs.write_bytes(content)

        start_events = []
        task = HttpsDownloadTask("t", [file], fs=fs)
        task.on_start(lambda e: start_events.append(e))
        result = asyncio.run(task.run())

        assert len(start_events[0].already_done) == 1
        assert start_events[0].files == []
        assert result.files == []

    def test_checksum_mismatch_file_not_at_drs(self, fs):
        content = b"tampered"
        file = make_https_file(content)
        file.checksum = "0" * 64
        asyncio.run(
            HttpsDownloadTask("t", [file], fs=fs, client=success_client(content)).run()
        )
        assert not fs[file].drs.is_file()

    def test_checksum_mismatch_temp_files_cleaned_up(self, fs):
        content = b"tampered"
        file = make_https_file(content)
        file.checksum = "0" * 64
        asyncio.run(
            HttpsDownloadTask("t", [file], fs=fs, client=success_client(content)).run()
        )
        assert not fs[file].done.is_file()
        assert not fs[file].tmp.is_file()
