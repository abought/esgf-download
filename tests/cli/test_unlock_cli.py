from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from esgpull.cli.unlock import unlock
from esgpull.config import Config
from esgpull.constants import LOCK_FILENAME
from esgpull.lock import LockInfo


def _write_lock(root: Path) -> None:
    (root / LOCK_FILENAME).write_text(LockInfo.current().to_json())


def test_unlock_no_lock_file(root: Path, config: Config):
    config.generate(overwrite=True)
    runner = CliRunner()
    result = runner.invoke(unlock, [])
    assert result.exit_code == 0
    assert "No lock file found" in result.output


def test_unlock_with_yes_flag(root: Path, config: Config):
    config.generate(overwrite=True)
    _write_lock(root)
    runner = CliRunner()
    result = runner.invoke(unlock, ["--yes"])
    assert result.exit_code == 0
    assert not (root / LOCK_FILENAME).exists()


def test_unlock_confirm_accept(root: Path, config: Config):
    config.generate(overwrite=True)
    _write_lock(root)
    runner = CliRunner()
    result = runner.invoke(unlock, [], input="y\n")
    assert result.exit_code == 0
    assert not (root / LOCK_FILENAME).exists()


def test_unlock_confirm_decline(root: Path, config: Config):
    config.generate(overwrite=True)
    _write_lock(root)
    runner = CliRunner()
    result = runner.invoke(unlock, [], input="n\n")
    assert result.exit_code != 0
    assert (root / LOCK_FILENAME).exists()