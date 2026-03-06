"""
Factory functions for creating download tasks from a list of files.
"""
from typing import TYPE_CHECKING

from jedi.inference.value.iterable import Sequence

from esgpull.downloader.as_globus import GlobusDownloadTask, GlobusTaskStartInfo
from esgpull.downloader.as_https import HttpsDownloadTask
from esgpull.models import File
from esgpull.models.globus_transfer import GlobusTransfer, GlobusTransferStatus

if TYPE_CHECKING:
    from globus_sdk import TransferClient
    from esgpull.esgpull import Esgpull


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
    return [
        HttpsDownloadTask(task_label=file.file_id, files=[file], fs=app.fs)
        for file in files
    ]


def make_globus_tasks(
    globus_files: dict[str, list[File]],
    app: 'Esgpull',
    transfer_client: 'TransferClient',
) -> list[GlobusDownloadTask]:
    """
    Create one GlobusDownloadTask per source collection, with an on_start callback
    that persists a GlobusTransfer record (and associated files) to the database.
    """
    tasks = []
    for origin_id, batch in globus_files.items():
        task = GlobusDownloadTask(
            task_label=origin_id,
            files=batch,
            client=transfer_client,
            source_collection_id=origin_id,
            dest_collection_id=app.config.globus.destination_collection_uuid,
            dest_root_path=app.config.globus.destination_collection_root,
        )
        task.on_start(_make_globus_on_start(app))
        tasks.append(task)
    return tasks


def _make_globus_on_start(app: 'Esgpull'):
    def on_start(start_info: GlobusTaskStartInfo) -> None:
        transfer = GlobusTransfer(
            task_id=start_info.globus_task_id,
            status=GlobusTransferStatus.ACTIVE,
        )
        transfer.files = list(start_info.files)
        with app.db.commit_context():
            app.db.session.add(transfer)

    return on_start
