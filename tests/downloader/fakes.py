"""Shared test doubles for downloader unit tests."""
from esgpull.downloader.base import DownloadTask, FileResult, TaskResultEvent, TaskStatus
from esgpull.models import File, FileStatus
from esgpull.models.globus_storage import GlobusStorage


def make_file(
    size: int = 0,
    file_id: str = "file",
    filename: str | None = None,
    globus_origin_path: str | None = None,
) -> File:
    f = File(
        file_id=file_id,
        dataset_id="dataset",
        master_id="master",
        url=f"https://example.com/{file_id}",
        version="v0",
        filename=filename or f"{file_id}.nc",
        local_path="project/folder",
        data_node="data_node",
        checksum="0",
        checksum_type="0",
        size=size,
        status=FileStatus.Queued,
    )
    if globus_origin_path is not None:
        f.globus_storage = GlobusStorage(origin_id="source-collection-uuid", origin_path=globus_origin_path)
    f.compute_sha()
    return f


class FakeDownloadTask(DownloadTask):
    """Configurable concrete subclass for testing DownloadTask.

    All keyword arguments are optional; omitting them gives a task that
    passes pre-check, succeeds, and passes cleanup unchanged.
    """

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
        return self._make_result(TaskStatus.COMPLETE, "ok", [])

    async def _cleanup(self, result: TaskResultEvent) -> TaskResultEvent:
        return result

    def to_cancel(self) -> TaskResultEvent:
        return self._make_result(TaskStatus.CANCELED, "cancelled", [])


class FakeHeartbeatTask(FakeDownloadTask):
    """Variant that emits one heartbeat per run."""

    async def _run(self, to_download, skip):
        self._emit_heartbeat(1, 0)
        return self._make_result(TaskStatus.COMPLETE, "ok", [])