"""
Factory functions for creating download tasks from a list of files.
"""
from collections.abc import Sequence
from typing import TYPE_CHECKING

from esgpull.downloader.as_globus import GlobusTransferTask
from esgpull.downloader.as_https import HttpsDownloadTask
from esgpull.downloader.orchestrator import Orchestrator
from esgpull.models import File
from esgpull.models.globus_transfer import GlobusTransfer, GlobusTransferStatus

if TYPE_CHECKING:
    from globus_sdk import TransferClient
    from esgpull.esgpull import Esgpull
    from esgpull.downloader.ui import HttpsDownloadUI


def partition_by_transfer_method(files: Sequence[File]) -> tuple[list[File], dict[str, list[File]]]:
    """
    Partition files into those that support Globus transfer and those that do not.

    Returns:
        globus_files: files grouped by source collection ID (origin_id)
        https_files: files without a Globus source
    """
    https_files: list[File] = []
    globus_files: dict[str, list[File]] = {}

    for file in files:
        if file.globus_storage is not None:
            globus_files.setdefault(file.globus_storage.origin_id, []).append(file)
        else:
            https_files.append(file)

    return https_files, globus_files,


def make_https_tasks(files: list[File], app: 'Esgpull') -> list[HttpsDownloadTask]:
    """
    Create one HttpsDownloadTask per file.

    A separate task per file allows the orchestrator's worker pool to
    download multiple files in parallel.
    """
    cfg = app.config.download
    return [
        HttpsDownloadTask(
            task_label=file.file_id,
            files=[file],
            fs=app.fs,
            chunk_size=cfg.chunk_size,
            disable_checksum=cfg.disable_checksum,
            disable_ssl=cfg.disable_ssl,
            http_timeout=cfg.http_timeout,
        )
        for file in files
    ]


def add_https_tasks(
    orch: Orchestrator,
    files: list[File],
    app: 'Esgpull',
    ui: 'HttpsDownloadUI',
) -> None:
    """
    Prepare url-based downloads and add appropriate callbacks
    """
    for task in make_https_tasks(files, app):
        orch.add_local_task(task)
    orch.on_task_start(ui.on_start)
    orch.on_heartbeat(ui.on_heartbeat)


def make_globus_tasks(
    globus_files: dict[str, list[File]],
    app: 'Esgpull',
    transfer_client: 'TransferClient',
) -> list[GlobusTransferTask]:
    """
    Create one GlobusTransferTask per source collection, with lifecycle callbacks
    that persist GlobusTransfer records to the database on start and completion.
    """
    cfg = app.config.download
    tasks = []
    for origin_id, batch in globus_files.items():
        task = GlobusTransferTask(
            task_label=origin_id,
            files=batch,
            client=transfer_client,
            source_collection_id=origin_id,
            dest_collection_id=app.config.globus.destination_collection_uuid,
            dest_root_path=app.config.globus.destination_collection_root,
            wait_until_resolved=cfg.poll_globus,
            poll_time_max=cfg.poll_globus_time_max,
        )
        task.on_start(_make_globus_on_start(app))
        task.on_result(_make_globus_on_result(app))
        tasks.append(task)
    return tasks


def _make_globus_on_start(app: 'Esgpull'):
    from esgpull.downloader.base import TaskStartEvent

    def on_start(start_info: TaskStartEvent) -> None:
        task_id = start_info.extra['globus_task_id']
        transfer = GlobusTransfer(
            task_id=task_id,
            status=GlobusTransferStatus.ACTIVE,
        )
        transfer.files = list(start_info.files)
        with app.db.commit_context():
            app.db.session.add(transfer)

    return on_start


def _make_globus_on_result(app: 'Esgpull'):
    from datetime import datetime, timezone
    from esgpull.downloader.base import TaskResultEvent

    def on_result(result: TaskResultEvent) -> None:
        task_id = result.extra.get('globus_task_id')
        if task_id is None:
            # This branch triggers if task failed to submit to globus
            return
        transfer = app.db.session.get(GlobusTransfer, task_id)
        if transfer is None:
            return
        globus_status = result.extra.get('globus_task_status')
        if globus_status is not None:
            transfer.status = globus_status
        transfer.last_updated = datetime.now(timezone.utc)
        transfer.completion_time = result.end_time
        with app.db.commit_context():
            app.db.session.add(transfer)

    return on_result
