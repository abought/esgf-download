from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import opts
from esgpull.cli.utils import init_esgpull
from esgpull.lock import read_lock_info
from esgpull.tui import Verbosity


@click.command
@opts.yes
@opts.verbosity
def unlock(yes: bool, verbosity: Verbosity) -> None:
    """
    Release the lock used by download/update/retry

    WARNING: if two instances of esgpull are running at once, they could conflict and cause download tasks to break.
        Before unlocking, use the provided PID info to verify that the other instance (such as a cron job) is stopped.
    """
    esg = init_esgpull(verbosity)
    with esg.ui.logging("unlock", onraise=Abort):
        info = read_lock_info(esg.path)
        if info is None:
            esg.ui.print("No lock file found.")
            raise Exit(0)
        esg.ui.print(
            f"Lock held by [bold]{info.hostname}[/] (pid={info.pid}),"
            f" running `{info.command}`,"
            f" since {info.created_at.isoformat(timespec='seconds')} UTC."
        )
        if not yes and not esg.ui.ask("Remove this lock?", default=False):
            raise Abort
        esg.lock().release()
        esg.ui.print(":+1: Lock removed.")