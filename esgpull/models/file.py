from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from typing_extensions import NotRequired, TypedDict

from esgpull.models.base import Base
from esgpull.models.utils import find_str


class FileStatus(Enum):
    New = "new"          # ORM default; immediately overwritten to Queued in normal flows
    Queued = "queued"    # Eligible for download or retry
    Starting = "starting"  # Task scheduled
    Started = "started"    # Task running
    Pausing = "pausing"  # Possibly unused
    Paused = "paused"    # Possibly unused
    Error = "error"      # download failed (retryable)
    Cancelled = "cancelled"  # interrupted before completion (retryable)
    Done = "done"        # file successfully downloaded

    @classmethod
    def retryable(cls) -> list[FileStatus]:
        return [cls.Error, cls.Cancelled]

    @classmethod
    def contains(cls, s: str) -> bool:
        return s in [v.value for v in cls]


class FileDict(TypedDict):
    file_id: str
    dataset_id: str
    master_id: str
    url: str
    version: str
    filename: str
    local_path: str
    data_node: str
    checksum: str
    checksum_type: str
    size: int
    status: NotRequired[str]


@dataclass(init=False)
class FastFile:
    sha: str
    file_id: str
    checksum: str

    def _as_bytes(self) -> bytes:
        self_tuple = (self.file_id, self.checksum)
        return str(self_tuple).encode()

    @classmethod
    def serialize(cls, source: dict) -> FastFile:
        result = cls()
        dataset_id = find_str(source["dataset_id"]).partition("|")[0]
        filename = find_str(source["title"])
        result.file_id = ".".join([dataset_id, filename])
        result.checksum = find_str(source["checksum"])
        Base.compute_sha(result)  # type: ignore
        return result
