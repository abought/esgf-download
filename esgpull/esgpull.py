from __future__ import annotations

import asyncio
import logging
import shutil
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property, partial
from pathlib import Path
from warnings import warn

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

from esgpull.config import Config
from esgpull.context import Context
from esgpull.database import Database

from esgpull.downloader.as_globus import GlobusStatusTask
from esgpull.downloader.as_https import DownloadCtx, check_disk_space
from esgpull.downloader.base import TaskResultEvent
from esgpull.downloader.callbacks import (
    check_globus_auth_error,
    is_disk_full,
    make_file_state_on_result,
    make_globus_transfer_on_result,
    make_on_task_start,
)
from esgpull.downloader.factory import add_https_tasks, make_globus_tasks, partition_by_transfer_method
from esgpull.downloader.orchestrator import Orchestrator
from esgpull.downloader.ui import GlobusDownloadUI, GlobusPrecheckUI, HttpsDownloadUI
from esgpull.exceptions import (
    DownloadCancelled,
    InsufficientDiskSpace,
    InvalidInstallPath,
    NoInstallPath,
    UnknownDefaultQueryID,
)
from esgpull.downloader.fs import Filesystem
from esgpull.globus.transfer import get_transfer_client
from esgpull.graph import Graph
from esgpull.install_config import InstallConfig
from esgpull.models import (
    Facet,
    File,
    FileStatus,
    GlobusTransferStatus,
    LegacyQuery,
    Options,
    Query,
    sql,
)
from esgpull.models.utils import short_sha
from esgpull.plugin import (
    Event,
    PluginManager,
    emit,
    get_plugin_manager,
    set_plugin_manager,
)
from esgpull.downloader.pipeline import Processor
from esgpull.result import Err, Ok, Result
from esgpull.tui import UI, DummyLive, ErrorCountColumn, Verbosity, logger
from esgpull.utils import format_size


@dataclass(repr=False)
class Esgpull:
    path: Path
    config: Config
    ui: UI
    db: Database
    context: Context
    fs: Filesystem
    graph: Graph

    def __init__(
        self,
        path: Path | str | None = None,
        verbosity: Verbosity = Verbosity.Detail,
        install: bool = False,
        record: bool = False,
        safe: bool = False,
        load_db: bool = True,
    ) -> None:
        if path is not None:
            path = Path(path)
            InstallConfig.choose(path=path)
            default = path
            warning = f"Using unknown location: {path}\n"
        else:
            default = InstallConfig.default
            warning = f"Using default location: {default}\n"
        if InstallConfig.current is None:
            if safe:
                raise NoInstallPath
            InstallConfig.choose(path=default)
            if InstallConfig.current_idx is None:
                idx = InstallConfig.add(default)
                InstallConfig.choose(idx=idx)
                needs_install = True
            else:
                idx = InstallConfig.current_idx
                needs_install = False
            self.path = InstallConfig.installs[idx].path
            warning += "To disable this warning, please run:\n"
            if needs_install:
                warning += f"$ esgpull self install {self.path}"
            else:
                warning += f"$ esgpull self choose {self.path}"
            if logger.level == logging.NOTSET:
                warn(warning)
            else:
                logger.warning(warning)
        else:
            self.path = InstallConfig.current.path
        if not install and not self.path.is_dir():
            raise InvalidInstallPath(path=self.path)
        self.config = Config.load(path=self.path)
        Options._set_defaults(**self.config.api.default_options.asdict())
        self.fs = Filesystem.from_config(self.config, install=install)
        self.ui = UI.from_config(
            self.config,
            verbosity=verbosity,
            record=record,
        )
        self.context = Context(self.config, noraise=True)
        if load_db:
            self.db = Database.from_config(self.config)
            self.graph = Graph(self.db)
        # Initialize plugin system
        plugin_config_path = self.config.paths.plugins / "plugins.toml"
        try:
            self.plugin_manager = get_plugin_manager()
            PluginManager.__init__(self.plugin_manager, config_path=plugin_config_path)
        except ValueError:
            self.plugin_manager = PluginManager(config_path=plugin_config_path)
            set_plugin_manager(self.plugin_manager)
        if self.config.plugins.enabled:
            self.plugin_manager.enabled = True
            self.config.paths.plugins.mkdir(exist_ok=True, parents=True)
            self.plugin_manager.discover_plugins(self.config.paths.plugins)

    def fetch_index_nodes(self) -> list[str]:
        """
        Returns a list of ESGF index nodes.

        Fetch hints from ESGF search API with a distributed query.
        """

        default_index = self.config.api.index_node
        logger.info(f"Fetching index nodes from {default_index!r}")
        options = Options(distrib=True)
        query = Query(options=options)
        facets = ["index_node"]
        hints = self.context.hints(
            query,
            file=False,
            facets=facets,
            index_node=default_index,
        )
        return list(hints[0]["index_node"])

    def fetch_facets(self, update: bool = False) -> bool:
        """
        Fill db with all existing facets found in ESGF index nodes.

        1. Fetch index nodes from `Esgpull.fetch_index_nodes()`
        2. Fetch all facets (names + values) from all index nodes.

        Workaround method, since searching directly for all facets using
        `distrib=True` seems to crash the index node.
        """

        # those facets have (almost) unique values
        IGNORE_NAMES = [
            "version",
            # "cf_standard_name",
            # "variable_long_name",
            "creation_date",
            # "datetime_end",
        ]
        nb_facets = self.db.scalars(sql.count_table(Facet))[0]
        logger.info(f"Found {nb_facets} facets in database")
        if nb_facets and not update:
            return False
        index_nodes = self.fetch_index_nodes()
        options = Options(distrib=False)
        query = Query(options=options)
        hints_coros = []
        for index_node in index_nodes:
            hints_results = self.context.prepare_hints(
                query,
                file=False,
                facets=["*"],
                index_node=index_node,
            )
            hints_coros.append(self.context._hints(*hints_results))
        hints = self.context.sync_gather(*hints_coros)
        new_facets: set[Facet] = set()
        facets_db = self.db.scalars(sql.facet.all())
        for index_hints in hints:
            for name, values in index_hints[0].items():
                if name in IGNORE_NAMES:
                    continue
                for value in values.keys():
                    facet = Facet(name=name, value=value)
                    if facet not in facets_db:
                        facet.compute_sha()
                        new_facets.add(facet)
        self.db.add(*new_facets)
        return len(new_facets) > 0

    @cached_property
    def legacy_query(self) -> Query:
        legacy = LegacyQuery
        if (
            legacy_db := self.db.get(Query, "LEGACY")
        ) and legacy_db is not None:
            legacy = legacy_db
        # else:
        # self.db.add(legacy)
        # self.graph.add(legacy, clone=False)
        # self.graph.merge(commit=True)
        return legacy

    def import_synda(
        self,
        url: Path,
        track: bool = False,
        size: int = 5000,
        ask: bool = False,
    ) -> int:
        assert url.is_file()
        synda = Database(f"sqlite:///{url}", run_migrations=False)
        synda_ids = synda.scalars(sql.synda_file.ids())
        shas = set(self.db.scalars(sql.file.linked()))
        msg = f"Found {len(synda_ids)} files to import, proceed?"
        if ask and not self.ui.ask(msg):
            return 0
        synda_shas: set[str] = set()
        idx_range = range(0, len(synda_ids), size)
        if track:
            iter_idx_range = self.ui.track(idx_range)
        else:
            iter_idx_range = iter(idx_range)
        nb_imported = 0
        for start in iter_idx_range:
            stop = min(len(synda_ids), start + size)
            ids = synda_ids[start:stop]
            synda_files = synda.scalars(sql.synda_file.with_ids(*ids))
            files: list[File] = []
            for synda_file in synda_files:
                file = synda_file.to_file()
                if file.sha not in shas:
                    file.queries.append(self.legacy_query)
                    files.append(file)
                    synda_shas.add(file.sha)
            if files:
                nb_imported += len(files)
                self.db.add(*files)
        return nb_imported

    # def add(
    #     self,
    #     *queries: Query,
    #     with_file: bool = False,
    # ) -> tuple[list[Query], list[Query]]:
    #     """
    #     Add new queries to query/options/selection tables.
    #     Returns two lists: added and discarded queries
    #     """
    #     for query in
    #     self.graph.add()
    #     return [], []

    # def install(
    #     self,
    #     *files: File,
    #     status: FileStatus = FileStatus.Queued,
    # ) -> tuple[list[File], list[File]]:
    #     """
    #     Insert `files` with specified `status` into db if not already there.
    #     """
    #     file_ids = [f.file_id for f in files]
    #     with self.db.select(File.file_id) as stmt:
    #         stmt.where(File.file_id.in_(file_ids))
    #         existing_file_ids = set(stmt.scalars)
    #     to_install = [f for f in files if f.file_id not in existing_file_ids]
    #     to_download: list[File] = []
    #     already_on_disk: list[File] = []
    #     for file in to_install:
    #         if status == FileStatus.Done:
    #             # skip check on status=done
    #             file.status = status
    #             to_download.append(file)
    #             continue
    #         path = self.fs.path_of(file)
    #         if path.is_file():
    #             file.status = FileStatus.Done
    #             already_on_disk.append(file)
    #         else:
    #             file.status = status
    #             to_download.append(file)
    #     self.db.add(*to_install)
    #     return to_download, already_on_disk

    # def remove(self, *files: File) -> list[File]:
    #     """
    #     Remove `files` from db and delete from filesystem.
    #     """
    #     file_ids = [f.file_id for f in files]
    #     with self.db.select(File) as stmt:
    #         stmt.where(File.file_id.in_(file_ids))
    #         deleted = stmt.scalars
    #     for file in files:
    #         if file.status == FileStatus.Done:
    #             self.fs.delete(file)
    #     self.db.delete(*deleted)
    #     return deleted

    # def autoremove(self) -> list[File]:
    #     """
    #     Search duplicate files and keep latest version only.
    #     """
    #     deprecated = self.db.get_deprecated_files()
    #     return self.remove(*deprecated)

    async def iter_results(
        self,
        processor: Processor,
        progress: Progress,
        task_ids: dict[str, TaskID],
        live: Live | DummyLive,
    ) -> AsyncIterator[Result[DownloadCtx]]:
        """Used by legacy download functionality"""
        async for result in processor.process():
            task_idx = progress.task_ids.index(task_ids[result.data.file.sha])
            task = progress.tasks[task_idx]
            progress.update(task.id, visible=True)
            match result:
                case Ok():
                    progress.update(task.id, completed=result.data.completed)
                    if task.finished:
                        # TODO: add checksum verif here
                        progress.stop_task(task.id)
                        progress.update(task.id, visible=False)
                        sha = f"[b blue]{task.fields['sha']}[/]"
                        file = result.data.file
                        digest = result.data.digest
                        match self.fs.finalize(file, digest=digest):
                            case Ok():
                                size = f"[green]{format_size(int(task.completed))}[/]"
                                if task.elapsed is not None:
                                    final_speed = int(
                                        task.completed / task.elapsed
                                    )
                                    speed = (
                                        f"[red]{format_size(final_speed)}/s[/]"
                                    )
                                else:
                                    speed = "[b red]?[/]"
                                data_node = (
                                    f"[blue]{task.fields['data_node']}[/]"
                                )
                                parts = [sha, size, speed, data_node]
                                if self.config.download.show_filename:
                                    parts.append(task.fields["filename"])
                                msg = " · ".join(parts)
                                logger.info(msg)
                                live.console.print(msg)
                                yield result
                            case Err(_, err):
                                progress.remove_task(task.id)
                                yield Err(result.data, err=err)
                case Err():
                    progress.remove_task(task.id)
                    yield result
                case _:
                    raise ValueError("Unexpected result")

    async def download(
        self,
        queue: list[File],
        use_db: bool = True,
        show_progress: bool = True,
    ) -> tuple[list[File], list[Err]]:
        """
        Download files provided in `queue`.
        """
        for file in queue:
            file.status = FileStatus.Starting
        main_progress = self.ui.make_progress(
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
        if self.config.download.show_filename:
            file_columns.extend(
                [
                    "·",
                    TextColumn("{task.fields[filename]}"),
                ]
            )
        file_progress = self.ui.make_progress(
            *file_columns,
            transient=True,
        )
        file_task_shas = {}
        start_callbacks = {}
        for file in queue:
            task_id = file_progress.add_task(
                "",
                total=file.size,
                visible=False,
                start=False,
                sha=short_sha(file.sha),
                filename=file.filename,
                data_node=file.data_node,
            )
            callback = partial(file_progress.start_task, task_id)
            file_task_shas[file.sha] = task_id
            start_callbacks[file.sha] = [callback]
        processor = Processor(
            config=self.config,
            fs=self.fs,
            files=queue,
            start_callbacks=start_callbacks,
        )
        if use_db:
            self.db.add(*processor.files)
        queue_size = len(processor.tasks)
        main_task_id = main_progress.add_task(
            "", total=queue_size, nb_errors=0
        )
        # TODO: rename ? installed/downloaded/completed/...
        files: list[File] = []
        errors: list[Err] = []
        remaining_dict = {file.sha: file for file in processor.files}
        try:
            with self.ui.live(
                file_progress,
                main_progress,
                disable=not show_progress,
            ) as live:
                async for result in self.iter_results(
                    processor,
                    file_progress,
                    file_task_shas,
                    live,
                ):
                    match result:
                        case Ok():
                            main_progress.update(main_task_id, advance=1)
                            file = result.data.file
                            file.status = FileStatus.Done
                            files.append(file)
                            emit(
                                Event.file_complete,
                                file=file,
                                destination=self.fs[file].drs,
                                start_time=result.data.start_time,
                                end_time=datetime.now(),
                            )
                            if file.dataset is not None:
                                is_dataset_complete = self.db.scalars(
                                    sql.dataset.is_complete(file.dataset)
                                )[0]
                                if is_dataset_complete:
                                    emit(
                                        Event.dataset_complete,
                                        dataset=file.dataset,
                                    )
                        case Err(_, err):
                            queue_size -= 1
                            result.data.file.status = FileStatus.Error
                            errors.append(result)
                            main_progress.update(
                                main_task_id,
                                total=queue_size,
                                nb_errors=len(errors),
                            )
                            emit(
                                Event.file_error,
                                file=result.data.file,
                                exception=err,
                            )

                    if use_db:
                        self.db.add(result.data.file)
                    remaining_dict.pop(result.data.file.sha, None)
        finally:
            if remaining_dict:
                logger.warning(f"Cancelling {len(remaining_dict)} downloads.")
                cancelled: list[File] = []
                for file in remaining_dict.values():
                    file.status = FileStatus.Cancelled
                    cancelled.append(file)
                    errors.append(Err(file, DownloadCancelled()))
                if use_db:
                    self.db.add(*cancelled)
        return files, errors

    async def download2_https(
        self,
        queue: list[File],  # TODO: Design q. Consider revisiting how eligible files are determined in replicator (cron) mode. Do we want to exclude canceled files until `esgpull retry` is run? Does that make sense for a hands-off process?
        use_db: bool = True,
        show_progress: bool = True,
    ) -> tuple[list[File], list[Err]]:
        """
        Download a series of files from a URL to a local filesystem
        """
        # Stop immediately if disk does not have enough space for the required downloads.
        check_disk_space(queue, self.fs)
        ui = HttpsDownloadUI(
            self.ui,
            len(queue),
            self.config.download.show_filename,
            show_progress,
        )
        # Eventually, both URL-based files and globus downloads will use the same orchestrator
        orch = Orchestrator(max_concurrent_local=self.config.download.max_concurrent)
        add_https_tasks(orch, queue, self, ui)
        orch.on_task_start(make_on_task_start(self.db, use_db))

        files: list[File] = []
        errors: list[Err] = []
        orch.on_result(make_file_state_on_result(self.db, use_db, files, errors))
        orch.on_result(ui.on_result)

        gen = orch.iter_results()
        try:
            with ui.live():
                async for event in gen:
                    if is_disk_full(event):
                        await gen.aclose()
                        await orch.collect_cancels()
                        needed = sum(file.size for file in queue)
                        free = shutil.disk_usage(self.fs.paths.data).free
                        raise InsufficientDiskSpace(
                            self.fs.paths.data,
                            format_size(needed),
                            format_size(free),
                        )
        except (KeyboardInterrupt, asyncio.CancelledError):
            await orch.collect_cancels()
            raise
        return files, errors

    async def check_existing_globus_transfers(
        self,
        transfer_client: 'TransferClient',
        use_db: bool = True,
        show_progress: bool = True,
    ) -> tuple[list[File], list[Err]]:
        """
        Check all non-terminal Globus transfers once (no polling) and update their file statuses.
        Called before submitting new transfers so that previously submitted work is accounted for first.

        Only runs when `config.download.prefer_globus` is enabled. When disabled, existing
        transfers are intentionally left unchecked — Globus may have been turned off along with
        its credentials, and it becomes the job of `esgpull update`/`esgpull retry` to make those
        files eligible for download again under whichever method is currently active.

        Since this only runs with `prefer_globus` enabled, Globus is required for basic
        functionality: any un-retryable failure to check a transfer's status (eg an auth error) stops the
        program by letting `GlobusAuthError` propagate.
        """
        files: list[File] = []
        errors: list[Err] = []
        if not self.config.download.prefer_globus:
            return files, errors

        pending_transfers = self.db.scalars(sql.globus_transfer.pending())
        if not pending_transfers:
            return files, errors

        ui = GlobusPrecheckUI(self.ui, len(pending_transfers), show_progress)
        orch = Orchestrator(max_concurrent_remote=3)  # Globus limit: 3 concurrent active transfers
        for transfer in pending_transfers:
            task = GlobusStatusTask(
                task_label=transfer.task_id,
                files=list(transfer.files),
                client=transfer_client,
                transfer_task_id=transfer.task_id,
                wait_until_resolved=False,
                poll_time_max=self.config.download.poll_globus_time_max,
            )
            orch.add_remote_task(task)
        orch.on_task_start(make_on_task_start(self.db, use_db))
        orch.on_result(make_file_state_on_result(self.db, use_db, files, errors))
        orch.on_result(make_globus_transfer_on_result(self.db))
        orch.on_result(ui.on_result)

        gen = orch.iter_results()
        try:
            with ui.live():
                async for event in gen:
                    check_globus_auth_error(event, self.config.globus.client_id)
        except (KeyboardInterrupt, asyncio.CancelledError):
            await orch.collect_cancels()
            raise

        return files, errors

    def fail_pending_globus_transfers(self, use_db: bool = True) -> list[File]:
        """
        Mark all non-terminal Globus transfers as failed, and re-queue their files for
        whatever download mechanism is currently allowed.

        Used by `esgpull retry`/`esgpull update` when `config.download.prefer_globus` is
        disabled: if a user turned off globus mode, the file download should switch to other methods.
        """
        files: list[File] = []
        for transfer in self.db.scalars(sql.globus_transfer.pending()):
            transfer.status = GlobusTransferStatus.FAILED
            transfer_files = list(transfer.files)
            for file in transfer_files:
                file.status = FileStatus.Queued
                file.globus_transfer_task_id = None
            files.extend(transfer_files)
            if use_db:
                self.db.add(transfer, *transfer_files)
        return files

    async def download3_globus(
        self,
        transfer_client: 'TransferClient',
        queue: list[File],
        use_db: bool = True,
        show_progress: bool = True,
    ) -> tuple[list[File], list[Err]]:
        """
        Check in-progress Globus transfers for resolution, then submit new ones for files in queue.
        """
        files, errors = await self.check_existing_globus_transfers(transfer_client, use_db, show_progress)

        if not queue:
            return files, errors

        _, globus_files = partition_by_transfer_method(queue)
        if not globus_files:
            return files, errors

        ui = GlobusDownloadUI(
            self.ui,
            len(globus_files),
            show_task_bars=self.config.download.poll_globus,
            show_progress=show_progress,
        )
        globus_tasks = make_globus_tasks(globus_files, self, transfer_client, ui)

        orch = Orchestrator(max_concurrent_remote=3)  # Globus limit: 3 concurrent active transfers
        for task in globus_tasks:
            orch.add_remote_task(task)
        orch.on_task_start(make_on_task_start(self.db, use_db))
        orch.on_result(make_file_state_on_result(self.db, use_db, files, errors))
        orch.on_result(make_globus_transfer_on_result(self.db))
        orch.on_result(ui.on_result)

        gen = orch.iter_results()
        try:
            with ui.live():
                async for event in gen:
                    check_globus_auth_error(event, self.config.globus.client_id)
        except (KeyboardInterrupt, asyncio.CancelledError):
            await orch.collect_cancels()
            raise

        return files, errors

    async def download4_combined(
        self,
        queue: list[File],
        transfer_client: 'TransferClient | None' = None,
        use_db: bool = True,
        show_progress: bool = True,
    ) -> tuple[list[File], list[Err]]:
        """
        Download a specified list of files. Chooses the transfer method based on user-selected `config` options.
        """
        files: list[File] = []
        errors: list[Err] = []
        if not queue:
            return files, errors

        # Only split out Globus-eligible files when we'd actually use Globus for them.
        # Every file has a plain HTTPS url regardless of whether it also has Globus
        # storage metadata, so with Globus disabled everything just downloads via HTTPS
        # instead of silently dropping the Globus-tagged share of the queue.
        globus_files: dict[str, list[File]]
        if self.config.download.prefer_globus:
            https_files, globus_files = partition_by_transfer_method(queue)
        else:
            https_files, globus_files = list(queue), {}

        if https_files:
            # Files are downloaded to a local disk that we can introspect- run sanity checks
            check_disk_space(https_files, self.fs)

        orch = Orchestrator(
            max_concurrent_local=self.config.download.max_concurrent,
            max_concurrent_remote=3,  # Globus limit: 3 concurrent active transfers
        )

        https_ui: HttpsDownloadUI | None = None
        if https_files:
            https_ui = HttpsDownloadUI(
                self.ui,
                len(https_files),
                self.config.download.show_filename,
                show_progress,
            )
            add_https_tasks(orch, https_files, self, https_ui)

        globus_ui: GlobusDownloadUI | None = None
        if globus_files:
            if not transfer_client:
                transfer_client = get_transfer_client(self.config)
            globus_ui = GlobusDownloadUI(
                self.ui,
                len(globus_files),
                show_task_bars=self.config.download.poll_globus,
                show_progress=show_progress,
            )
            for task in make_globus_tasks(globus_files, self, transfer_client, globus_ui):
                orch.add_remote_task(task)

        if https_ui is None and globus_ui is None:
            return files, errors

        # File and task state tracking
        orch.on_task_start(make_on_task_start(self.db, use_db))
        orch.on_result(make_file_state_on_result(self.db, use_db, files, errors))
        orch.on_result(make_globus_transfer_on_result(self.db))

        def _route_result(event: TaskResultEvent) -> None:
            if 'globus_task_id' in event.extra and globus_ui is not None:
                globus_ui.on_result(event)
            elif https_ui is not None:
                https_ui.on_result(event)

        orch.on_result(_route_result)

        live_renderables: list[Progress] = []
        if https_ui is not None:
            live_renderables += [https_ui.file_progress, https_ui.main_progress]
        if globus_ui is not None:
            live_renderables += [globus_ui.task_progress, globus_ui.main_progress]

        gen = orch.iter_results()
        try:
            with self.ui.live(*live_renderables, disable=not show_progress) as live:
                if https_ui is not None:
                    https_ui._live = live
                if globus_ui is not None:
                    globus_ui._live = live
                async for event in gen:
                    # Most result handling is done by callbacks. This block handles special cases where a single
                    #   forced task should force ALL downloads to stop immediately.
                    check_globus_auth_error(event, self.config.globus.client_id)
                    if is_disk_full(event):
                        await gen.aclose()
                        await orch.collect_cancels()
                        needed = sum(file.size for file in https_files)
                        free = shutil.disk_usage(self.fs.paths.data).free
                        raise InsufficientDiskSpace(
                            self.fs.paths.data,
                            format_size(needed),
                            format_size(free),
                        )
        except (KeyboardInterrupt, asyncio.CancelledError):
            await orch.collect_cancels()
            raise

        return files, errors

    def replace_queries(
        self,
        graph: Graph,
        mapping: tuple[str | None, str],
    ) -> None:
        to_replace = [
            q for q in graph.queries.values() if q.require == mapping[0]
        ]
        for query in to_replace:
            new_query = query.clone(compute_sha=False)
            new_query.require = mapping[1]
            new_query.compute_sha()
            graph.replace(query, new_query)
            self.replace_queries(graph, (query.sha, new_query.sha))

    def insert_default_query(self, *queries: Query) -> list[Query]:
        if self.config.api.default_query_id == "":
            return list(queries)
        default_query_id = self.config.api.default_query_id
        try:
            default_query = self.graph.get(default_query_id)
        except KeyError:
            raise UnknownDefaultQueryID(default_query_id)
        graph = Graph(None)
        graph.add(*queries)
        self.replace_queries(graph, (None, default_query.sha))
        return list(graph.queries.values())
