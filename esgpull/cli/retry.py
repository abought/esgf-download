from collections import Counter
from collections.abc import Sequence

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import init_esgpull
from esgpull.models import File, FileStatus, sql
from esgpull.tui import Verbosity


@click.command()
@args.status
@opts.verbosity
def retry(
    status: Sequence[FileStatus],
    verbosity: Verbosity,
):
    """
    Re-queue failed and cancelled downloads
    """
    if not status:
        status = FileStatus.retryable()
    esg = init_esgpull(verbosity)
    with esg.ui.logging("retry", onraise=Abort):
        assert FileStatus.Done not in status
        assert FileStatus.Queued not in status
        files = list(esg.db.scalars(sql.file.with_status(*status)))
        # Rare edge case: If globus is enabled, and then disabled, any transfers in progress will be re-queued for
        #   https download
        stale_files: list[File] = []
        if not esg.config.download.prefer_globus:
            explicit_shas = {file.sha for file in files}
            stale_files = [
                file
                for file in esg.fail_pending_globus_transfers()
                if file.sha not in explicit_shas
            ]

        status_str = "/".join(f"[bold red]{s.value}[/]" for s in status)
        if not files and not stale_files:
            esg.ui.print(f"No {status_str} files found.")
            raise Exit(0)
        counts = Counter(file.status for file in files)
        for file in files:
            file.status = FileStatus.Queued
            file.globus_transfer_task_id = None
        esg.db.add(*files)
        parts = [
            f"{count} [bold red]{status.value}[/]"
            for status, count in counts.items()
        ]
        if stale_files:
            parts.append(
                f"{len(stale_files)} [bold red]Globus mode was disabled. Unresolved Globus transfer(s)[/]"
            )
        esg.ui.print("Sent back to the queue: " + ", ".join(parts))
