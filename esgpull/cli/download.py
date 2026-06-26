import asyncio

import click
import rich
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.exceptions import InsufficientDiskSpace
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
        shas: set[str] = set()
        queue: list[File] = []
        for query in graph.queries.values():
            for file in query.files:
                if file.status == FileStatus.Queued and file.sha not in shas:
                    shas.add(file.sha)
                    queue.append(file)
        if not queue:
            rich.print("Download queue is empty.")
            esg.ui.raise_maybe_record(Exit(0))
        try:
            coro = esg.download2_https(queue, show_progress=not quiet)
            files, errors = asyncio.run(coro)
        except InsufficientDiskSpace as exc:
            # Local system problem (lockfile conflict / disk full), not a
            # remote-server issue: alert a sysadmin distinctly from per-file
            # download failures.
            logger.error(str(exc))
            esg.ui.raise_maybe_record(Exit(2))
            return
        if files:
            size = format_size(sum(file.size for file in files))
            esg.ui.print(
                f"Downloaded {len(files)} new files for a total size of {size}"
            )
        # TODO: revisit — currently logs all results after the fact; consider
        #   logging each file outcome as it completes (via a result callback),
        #   so that we can track partial progress if a download is interrupted
        if errors:
            logger.error(f"{len(errors)} files could not be installed.")
            for err in errors:
                logger.error(
                    f"  {err.data.filename} [{err.data.data_node}]"
                    f" [{err.data.status.name}]: {err.err}"
                )
            # Some failures: likely a transient remote-server issue (exit 1).
            # All failures: likely a local system/usage problem (exit 2).
            exit_code = 1 if files else 2
            esg.ui.raise_maybe_record(Exit(exit_code))
        esg.ui.raise_maybe_record(Exit(0))
