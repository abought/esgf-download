import asyncio
import logging
from typing import AsyncGenerator

from esgpull.downloader.base import DownloadTask, TaskResult, StartCallback, HeartbeatCallback


class Orchestrator:
    """
    Orchestrate download tasks across two independent worker pools:

      local queue  — each file consumes local resources during transfer
      remote queue   — managed fire-and-forget transfers via a remote service (such as globus)

    """
    def __init__(
        self,
        max_concurrent_local: int = 5,
        max_concurrent_remote: int = 3,  # globus allows 3 concurrent transfers
    ):
        self._local_queue: asyncio.Queue[DownloadTask] = asyncio.Queue()
        self._remote_queue: asyncio.Queue[DownloadTask] = asyncio.Queue()
        self._task_results: asyncio.Queue[TaskResult] = asyncio.Queue()

        self._max_concurrent_local = max_concurrent_local
        self._max_concurrent_remote = max_concurrent_remote

        self._workers: list[asyncio.Task] = []

        self._start_callbacks: list[StartCallback] = []
        self._heartbeat_callbacks: list[HeartbeatCallback] = []

    def add_local_task(self, task: DownloadTask) -> None:
        """Enqueue a task that performs direct, resource-intensive local work (eg HTTPS downloads)."""
        self._local_queue.put_nowait(task)

    def add_remote_task(self, task: DownloadTask) -> None:
        """Enqueue a task that dispatches work to an external service and polls for results (eg Globus transfers)."""
        self._remote_queue.put_nowait(task)

    async def _worker(self, queue: asyncio.Queue[DownloadTask]) -> None:
        while True:
            task = await queue.get()

            for cb in self._start_callbacks:
                task.on_start(cb)
            for hb in self._heartbeat_callbacks:
                task.on_heartbeat(hb)

            try:
                result = await task.run()
                self._task_results.put_nowait(result)

            except (asyncio.CancelledError, KeyboardInterrupt):
                # Worker was canceled by event loop or a synchronous SIGINT
                failure = task.to_cancel()
                self._task_results.put_nowait(failure)
                raise
            except Exception as err:
                # Unhandled conditions should mark everything in the task as failed
                logging.exception('An unknown error occurred')
                failure = task.to_fail(str(err))
                self._task_results.put_nowait(failure)
            except BaseException as err:
                failure = task.to_fail(str(err))
                self._task_results.put_nowait(failure)
                raise
            finally:
                queue.task_done()

    #### Public interface
    def on_heartbeat(self, cb: HeartbeatCallback) -> None:
        if cb not in self._heartbeat_callbacks:
            self._heartbeat_callbacks.append(cb)

    def on_task_start(self, cb: StartCallback) -> None:
        """
        Allow external consumers to register "start callbacks", such as updating a UI progress bar or persisting
            a globus transfer task ID to the database

        NOTE: Callbacks can also be registered at the task level, if they require specific behaviors for a task type
            (like "saving globus task ID to database").
        """
        if cb not in self._start_callbacks:
            self._start_callbacks.append(cb)

    async def iter_results(self) -> AsyncGenerator[TaskResult, None]:
        """
        Run tasks asynchronously and report results as available.=

        NOTE: See collect_cancels() to retrieve results for interrupted tasks  (CancelledError or KeyboardInterrupt)
        """
        n_local = self._local_queue.qsize()
        n_remote = self._remote_queue.qsize()
        n_expected = n_local + n_remote

        if n_expected == 0:
            return

        l_workers = [
            asyncio.create_task(self._worker(self._local_queue))
            for _ in range(min(n_local, self._max_concurrent_local))
        ]
        r_workers = [
            asyncio.create_task(self._worker(self._remote_queue))
            for _ in range(min(n_remote, self._max_concurrent_remote))
        ]
        self._workers =  l_workers + r_workers

        try:
            for _ in range(n_expected):
                yield await self._task_results.get()
        finally:
            for w in self._workers:
                w.cancel()

    async def collect_cancels(self) -> list[TaskResult]:
        """
        Ensures that canceled tasks report a valid result. MUST be called manually.

        Should be called from the caller's except block after iter_results is
        interrupted. Waits for any mid-task workers to finish their cancellation
        handlers (so their results land in the queue), then drains both the
        results queue and any tasks still waiting to be dequeued.
        """
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)

        results: list[TaskResult] = []
        while not self._task_results.empty():
            results.append(self._task_results.get_nowait())
        for queue in (self._local_queue, self._remote_queue):
            while not queue.empty():
                task = queue.get_nowait()
                results.append(task.to_cancel())
                queue.task_done()
        return results
