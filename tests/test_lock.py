from __future__ import annotations

import json
import os
import signal
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from esgpull.constants import LOCK_FILENAME
from esgpull.exceptions import LockedError
from esgpull.lock import ProcessLock, read_lock_info


@pytest.fixture(autouse=True)
def _ensure_root_exists(root: Path) -> None:
    # The `root` fixture only registers the install path; it doesn't create
    # the directory (that's normally a side effect of the `config` fixture).
    root.mkdir(parents=True, exist_ok=True)


def test_acquire_creates_lock_file(root: Path):
    lock = ProcessLock(root)
    lock.acquire()
    try:
        assert lock.path.is_file()
        info = read_lock_info(root)
        assert info is not None
        assert info.pid == os.getpid()
    finally:
        lock.release()
    assert not lock.path.exists()


def test_acquire_conflict_raises_locked_error(root: Path):
    first = ProcessLock(root)
    first.acquire()
    try:
        with pytest.raises(LockedError) as excinfo:
            ProcessLock(root).acquire()
        assert f"pid={os.getpid()}" in str(excinfo.value)
    finally:
        first.release()


def test_release_is_idempotent(root: Path):
    lock = ProcessLock(root)
    lock.release()
    lock.release()


def _write_lock(root: Path, age: timedelta) -> None:
    created_at = datetime.now(timezone.utc) - age
    (root / LOCK_FILENAME).write_text(
        json.dumps(
            {
                "hostname": "otherhost",
                "pid": 1234,
                "command": "esgpull download",
                "created_at": created_at.isoformat(),
            }
        )
    )


def test_stale_lock_hint(root: Path):
    _write_lock(root, timedelta(hours=25))
    with pytest.raises(LockedError) as excinfo:
        ProcessLock(root).acquire()
    assert "stale" in str(excinfo.value).lower()


def test_fresh_lock_has_no_stale_hint(root: Path):
    _write_lock(root, timedelta(minutes=5))
    with pytest.raises(LockedError) as excinfo:
        ProcessLock(root).acquire()
    assert "stale" not in str(excinfo.value).lower()


def test_context_manager_releases_lock_on_normal_exit(root: Path):
    lock = ProcessLock(root)
    with lock:
        assert lock.path.exists()
    assert not lock.path.exists()


def test_context_manager_releases_on_exception(root: Path):
    lock = ProcessLock(root)
    with pytest.raises(RuntimeError):
        with lock:
            assert lock.path.exists()
            raise RuntimeError("boom")
    assert not lock.path.exists()


def test_context_manager_installs_and_restores_sigterm_handler(root: Path):
    previous = signal.getsignal(signal.SIGTERM)
    with ProcessLock(root):
        assert signal.getsignal(signal.SIGTERM) != previous
    assert signal.getsignal(signal.SIGTERM) == previous


def test_context_manager_releases_lock_on_sigterm(root: Path):
    lock = ProcessLock(root)
    with pytest.raises(SystemExit):
        with lock:
            assert lock.path.exists()
            os.kill(os.getpid(), signal.SIGTERM)
    assert not lock.path.exists()