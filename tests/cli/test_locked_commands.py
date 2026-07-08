from __future__ import annotations

import itertools
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from esgpull.cli.download import download
from esgpull.cli.retry import retry
from esgpull.cli.update import update
from esgpull.config import Config
from esgpull.constants import LOCK_FILENAME
from esgpull.lock import LockInfo

LOCK_PROTECTED_COMMANDS: list[click.Command] = [update, download, retry]


def _write_lock_for(root: Path, holder: click.Command) -> None:
    info = LockInfo.current()
    info.command = f"esgpull {holder.name}"
    (root / LOCK_FILENAME).write_text(info.to_json())


@pytest.mark.parametrize(
    "locking_command,blocked_command",
    itertools.product(LOCK_PROTECTED_COMMANDS, LOCK_PROTECTED_COMMANDS),
    ids=lambda c: c.name,
)
def test_locked_commands_are_mutually_exclusive(
    root: Path,
    config: Config,
    locking_command: click.Command,
    blocked_command: click.Command,
):
    """
    `update`, `download`, and `retry` all check the same per-profile
    lockfile before doing any real work, so any one of them holding the
    lock blocks those commands from starting.
    """
    config.generate(overwrite=True)
    _write_lock_for(root, locking_command)
    runner = CliRunner()
    result = runner.invoke(blocked_command, [])
    assert result.exit_code == 1
    assert "Another instance appears to be running" in result.output