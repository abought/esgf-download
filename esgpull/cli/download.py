import asyncio

import click
import rich
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.exceptions import GlobusAuthError, InsufficientDiskSpace
from esgpull.globus.transfer import get_transfer_client
from esgpull.models import File, sql
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
    with esg.ui.logging("download", onraise=Abort), esg.lock():
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

        # RARE EDGE CASE: If `prefer_globus` is enabled, then disabled, existing Globus transfers are not checked at all.
        #   Use `esgpull retry` to requeue files for download.
        async def _run() -> tuple[list[File], list]:
            pre_files: list[File] = []
            pre_errors: list = []
            transfer_client = None

            if esg.config.download.prefer_globus:
                try:
                    transfer_client = get_transfer_client(esg.config)
                except Exception as client_exc:
                    logger.error(f"Globus auth/config error: {client_exc}")
                    esg.ui.raise_maybe_record(Exit(2))
                    return [], []

                pre_files, pre_errors = await esg.check_existing_globus_transfers(
                    transfer_client, show_progress=not quiet
                )

            # Query eligible files AFTER the precheck: resolving a pending transfer may
            # free its files up for (re)download (eg Started -> Error, now retry-eligible).
            # Error/Cancelled files are excluded here: per `esgpull retry`'s docs, they only
            # re-enter the queue via that explicit command, not a plain `esgpull download`.
            query_shas = list(graph.queries.keys())
            queue = list(esg.db.scalars(
                sql.file.ready_for_download(query_shas)
            ))

            new_files, new_errors = await esg.download4_combined(
                queue, transfer_client=transfer_client, show_progress=not quiet
            )
            return pre_files + new_files, pre_errors + new_errors

        try:
            files, errors = asyncio.run(_run())
        except (GlobusAuthError, InsufficientDiskSpace) as exc:
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
            exit_code = 1 if files else 2
            esg.ui.raise_maybe_record(Exit(exit_code))
        esg.ui.raise_maybe_record(Exit(0))