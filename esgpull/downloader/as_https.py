from __future__ import annotations

import errno
import os
import shutil
import ssl
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import contextlib

import aiofiles
import httpx
from httpx import AsyncClient

from esgpull.downloader.base import (
    DownloadTask,
    FileResult,
    TaskResultEvent, TaskStatus,
)
from esgpull.exceptions import InsufficientDiskSpace
from esgpull.models.file import FileStatus
from esgpull.downloader.fs import Digest, Filesystem
from esgpull.models import File
from esgpull.tui import logger
from esgpull.utils import format_size


def check_disk_space(files: list[File], fs: Filesystem) -> None:
    """
    Preflight check: ensure enough free space exists for `files`. Used for downloading to a local filesystem.

    `tmp` and `data` may be different filesystems (see `Filesystem.move_to_drs`'s
    fallback-to-copy handling), so both are checked; they're deduplicated by
    device id in case they share a filesystem.
    """
    needed = sum(file.size for file in files)
    devices: dict[int, Path] = {}
    for path in (fs.paths.tmp, fs.paths.data):
        devices[os.stat(path).st_dev] = path
    for path in devices.values():
        free = shutil.disk_usage(path).free
        if free < needed:
            raise InsufficientDiskSpace(path, format_size(needed), format_size(free))


def _make_ssl_context(disable_ssl: bool) -> ssl.SSLContext | bool:
    """
    Build an SSL context appropriate for the installed OpenSSL version.

    TODO: Are there still ESGF nodes that don't work with (modern/any) SSL? If not, can we omit this?

    """
    if disable_ssl:
        return False
    if ssl.OPENSSL_VERSION_INFO[0] >= 3:
        ctx = ssl.create_default_context()
        ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT — required for some ESGF nodes with older TLS
        return ctx
    return True


class HttpsDownloadTask(DownloadTask):
    """
    Download (usually one) item from a URL to a local file
    """

    def __init__(
        self,
        # Shared options
        task_label: str,
        files: list[File],
        # URL-specific options
        fs: Filesystem,
        chunk_size: int = 1 << 23,  # 8 MiB
        disable_checksum: bool = False,
        disable_ssl: bool = False,
        http_timeout: float = 120.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(task_label, files)
        self._fs = fs
        self._chunk_size = chunk_size
        self._disable_checksum = disable_checksum
        self._ssl_context = _make_ssl_context(disable_ssl)
        self._http_timeout = http_timeout
        self._client = client

    def to_cancel(self) -> TaskResultEvent:
        """When a local download is interrupted, mark file as eligible for retry later"""
        files = self._to_download or self._files
        return self._make_result(
            TaskStatus.CANCELED,
            'Download cancelled',
            [FileResult(FileStatus.Cancelled, f) for f in files]
        )

    async def _pre_check(self, files: list[File]) -> tuple[list[File], list[FileResult]]:
        """Skip files that already exist at their final DRS path."""
        to_download, already_done = [], []
        for file in files:
            if self._fs[file].drs.is_file():
                already_done.append(FileResult(FileStatus.Done, file))
            else:
                to_download.append(file)
        return to_download, already_done

    async def _run(self, to_download: list[File], skip: list[FileResult]) -> TaskResultEvent:
        results: list[FileResult] = []
        files_completed = len(skip)
        bytes_completed = sum(fr.file.size for fr in skip)

        client_ctx: contextlib.AbstractAsyncContextManager[httpx.AsyncClient]
        if self._client is not None:
            client_ctx = contextlib.nullcontext(self._client)
        else:
            client_ctx = httpx.AsyncClient(
                follow_redirects=True,
                verify=self._ssl_context,
                timeout=self._http_timeout,
            )
        try:
            async with client_ctx as client:
                for file in to_download:
                    file_path = self._fs[file]
                    digest = Digest(file) if not self._disable_checksum else None
                    status = FileStatus.Error

                    try:
                        async with aiofiles.open(file_path.tmp, 'wb') as f:
                            async with client.stream('GET', file.url) as resp:
                                resp.raise_for_status()
                                async for chunk in resp.aiter_bytes(chunk_size=self._chunk_size):
                                    await f.write(chunk)
                                    if digest is not None:
                                        digest.update(chunk)
                                    bytes_completed += len(chunk)
                                    self._emit_heartbeat(files_completed, bytes_completed)

                        file_path.tmp.rename(file_path.done)

                        if digest is None or digest.hexdigest() == file.checksum:
                            status = FileStatus.Done
                    except Exception as e:
                        if isinstance(e, OSError) and e.errno == errno.ENOSPC:
                            raise  # disk full affects all files — propagate as task-level failure
                        logger.exception(f"Download failed for file {file.file_id} in task {self._task_label}")

                    # Report the file as processed for progress tracking purposes
                    results.append(FileResult(status, file))
                    files_completed += 1
                    self._emit_heartbeat(files_completed, bytes_completed)

        except Exception as e:
            logger.exception(f"Task-level failure in task {self._task_label}")
            return self.to_fail(str(e), exception=e)

        return self._make_result(TaskStatus.COMPLETE, 'Download complete', results)

    async def _cleanup(self, result: TaskResultEvent) -> TaskResultEvent:
        """
        Move successfully downloaded files to their final DRS path.
        Delete any .done or .part temp files left behind by failures.
        """
        try:
            for fr in result.files:
                file_path = self._fs[fr.file]
                if fr.status == FileStatus.Done:
                    if file_path.done.is_file():
                        self._fs.move_to_drs(fr.file)
                else:
                    for path in (file_path.done, file_path.tmp):
                        if path.is_file():
                            path.unlink()
        except Exception as e:
            logger.exception(f"Filesystem error during cleanup for task {self._task_label}")
            return self.to_fail("Filesystem error during cleanup", exception=e)
        return result


# ---------------------------------------------------------------------------
# Legacy streaming helpers — retained for use by pipeline.py
# ---------------------------------------------------------------------------

@dataclass
class DownloadCtx:
    file: File
    completed: int = 0
    chunk: bytes | None = None
    digest: Digest | None = None
    start_time: datetime | None = None

    @property
    def finished(self) -> bool:
        return self.completed == self.file.size

    @property
    def error(self) -> bool:
        return self.completed > self.file.size

    def update_digest(self) -> None:
        if self.digest is not None and self.chunk is not None:
            self.digest.update(self.chunk)


class BaseDownloader:
    def stream(
        self,
        client: AsyncClient,
        ctx: DownloadCtx,
        chunk_size: int,
    ) -> AsyncGenerator[DownloadCtx, None]:
        raise NotImplementedError


class Simple(BaseDownloader):
    """
    Simple chunked async downloader.
    """

    async def stream(
        self,
        client: AsyncClient,
        ctx: DownloadCtx,
        chunk_size: int,
    ) -> AsyncGenerator[DownloadCtx, None]:
        ctx.start_time = datetime.now()
        async with client.stream("GET", ctx.file.url) as resp:
            resp.raise_for_status()
            async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                ctx.completed += len(chunk)
                ctx.chunk = chunk
                ctx.update_digest()
                yield ctx
