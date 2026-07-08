from __future__ import annotations

import json
import os
import signal
import socket
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import FrameType
from typing import Any

from esgpull.constants import LOCK_FILENAME
from esgpull.exceptions import LockedError

STALE_LOCK_THRESHOLD = timedelta(hours=24)


def _format_age(age: timedelta) -> str:
    seconds = int(age.total_seconds())
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h"
    return f"{hours // 24}d"


@dataclass
class LockInfo:
    hostname: str
    pid: int
    command: str
    created_at: datetime

    @classmethod
    def current(cls) -> LockInfo:
        return cls(
            hostname=socket.gethostname(),
            pid=os.getpid(),
            command=" ".join(sys.argv),
            created_at=datetime.now(timezone.utc),
        )

    def to_json(self) -> str:
        return json.dumps(
            {
                "hostname": self.hostname,
                "pid": self.pid,
                "command": self.command,
                "created_at": self.created_at.isoformat(),
            }
        )

    @classmethod
    def from_json(cls, content: str) -> LockInfo:
        data = json.loads(content)
        return cls(
            hostname=data["hostname"],
            pid=data["pid"],
            command=data["command"],
            created_at=datetime.fromisoformat(data["created_at"]),
        )


def read_lock_info(root: Path) -> LockInfo | None:
    try:
        content = (root / LOCK_FILENAME).read_text()
    except FileNotFoundError:
        return None
    return LockInfo.from_json(content)


def _raise_sigterm(signum: int, frame: FrameType | None) -> None:
    raise SystemExit(1)


@dataclass
class ProcessLock:
    root: Path
    _previous_handler: Any = field(default=None, init=False, repr=False)

    @property
    def path(self) -> Path:
        return self.root / LOCK_FILENAME

    def _locked_error(self) -> LockedError:
        info = read_lock_info(self.root)
        if info is None:
            # Lock file existed when we tried to create it, but is gone or
            # unreadable now (e.g. raced with a concurrent `esgpull unlock`).
            return LockedError(
                host="unknown",
                pid="unknown",
                command="unknown",
                created_at="unknown",
                age="unknown",
                stale_hint="",
            )
        age = datetime.now(timezone.utc) - info.created_at
        stale_hint = ""
        if age > STALE_LOCK_THRESHOLD:
            stale_hint = " This lock is over 24 hours old and may be stale."
        return LockedError(
            host=info.hostname,
            pid=info.pid,
            command=info.command,
            created_at=info.created_at.isoformat(timespec="seconds"),
            age=_format_age(age),
            stale_hint=stale_hint,
        )

    def acquire(self) -> None:
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise self._locked_error() from None
        with os.fdopen(fd, "w") as f:
            f.write(LockInfo.current().to_json())

    def release(self) -> None:
        self.path.unlink(missing_ok=True)

    def __enter__(self) -> ProcessLock:
        self.acquire()
        self._previous_handler = signal.signal(signal.SIGTERM, _raise_sigterm)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        signal.signal(signal.SIGTERM, self._previous_handler)
        self._previous_handler = None
        self.release()