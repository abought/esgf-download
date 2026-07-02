import asyncio

import click
import rich
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.exceptions import GlobusAuthError
from esgpull.globus.transfer import get_transfer_client
from esgpull.models import File, FileStatus
from esgpull.tui import Verbosity, logger
from esgpull.utils import format_size


@click.command()
@args.query_id
@opts.tag
@opts.disable_ssl
@opts.quiet
@opts.record
@opts.verbosity
def download(
    query_id: str | None,
    tag: str | None,
    disable_ssl: bool,
    quiet: bool,
    record: bool,
    verbosity: Verbosity,
):
    """
    Asynchronously download files linked to queries
    """
    esg = init_esgpull(verbosity, record=record)
    if disable_ssl:
        esg.config.download.disable_ssl = True
    with esg.ui.logging("download", onraise=Abort):
        if not valid_name_tag(esg.graph, esg.ui, query_id, tag):
            esg.ui.raise_maybe_record(Exit(1))
        if query_id is None and tag is None:
            esg.graph.load_db()
            graph = esg.graph
        else:
            queries = get_queries(esg.graph, query_id, tag)
            graph = esg.graph.subgraph(
                *queries,
                children=True,
                parents=True,
            )
        esg.ui.print(graph)

        # Per spec (download-combined.md): Resolve any pending Globus transfers
        # before querying for files eligible for download, even when prefer_globus
        # is off (transfers may have been submitted during a prior enabled period).
        try:
            transfer_client = get_transfer_client(esg.config)
        except Exception as exc:
            logger.error(f"Globus auth/config error: {exc}")
            esg.ui.raise_maybe_record(Exit(2))
            return

        async def _run() -> tuple[list[File], list]:
            pre_files, pre_errors = await esg._resolve_pending_globus_transfers(transfer_client)

            if not esg.config.download.prefer_globus:
                return pre_files, pre_errors

            shas: set[str] = set()
            queue: list[File] = []
            for query in graph.queries.values():
                for file in query.files:
                    if file.status == FileStatus.Queued and file.sha not in shas:
                        shas.add(file.sha)
                        queue.append(file)

            if not queue:
                return pre_files, pre_errors

            # download3_globus re-runs _resolve internally; that call will be a
            # no-op because all pending transfers were resolved above.
            new_files, new_errors = await esg.download3_globus(
                transfer_client, queue, show_progress=not quiet
            )
            return pre_files + new_files, pre_errors + new_errors

        try:
            files, errors = asyncio.run(_run())
        except GlobusAuthError as exc:
            logger.error(str(exc))
            esg.ui.raise_maybe_record(Exit(2))
            return

        if not files and not errors:
            rich.print("Download queue is empty.")
            esg.ui.raise_maybe_record(Exit(0))

        if files:
            size = format_size(sum(file.size for file in files))
            esg.ui.print(
                f"Downloaded {len(files)} new files for a total size of {size}"
            )
        if errors:
            logger.error(f"{len(errors)} files could not be downloaded.")
            for err in errors:
                source = (
                    f"globus:{err.data.globus_storage.origin_id}"
                    if err.data.globus_storage is not None
                    else err.data.data_node
                )
                logger.error(
                    f"  {err.data.filename} [{source}]"
                    f" [{err.data.status.name}]: {err.err}"
                )
            exit_code = 1 if files else 2
            esg.ui.raise_maybe_record(Exit(exit_code))
        esg.ui.raise_maybe_record(Exit(0))