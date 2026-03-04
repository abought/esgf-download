import asyncio
import logging
from typing import AsyncGenerator

from esgpull.downloader.base import DownloadTask, TaskResult, StartCallback, HeartbeatCallback


class Orchestrator:
    """
    Orchestrate download tasks across two independent worker pools:

      local queue  — for tasks that are resource-intensive locally (eg direct file downloads).
                      Kept small to avoid saturating the filesystem or network.
      remote queue   — for tasks that dispatch work to an external service and merely poll for
                      completion (eg Globus transfers). This can be managed separately from local tasks.

    Callers decide which queue a task belongs in via add_task(task, remote_queue=...).
    Results from both pools are available through iter_results().
    """
    def __init__(
        self,
        max_concurrent_local: int = 5,
        max_concurrent_remote: int = 3,  # globus allows 3 concurrent transfers
    ):
        self._local_queue: asyncio.Queue[DownloadTask] = asyncio.Queue()
        self._remote_queue: asyncio.Queue[DownloadTask] = asyncio.Queue()
        self._task_results: asyncio.Queue[TaskResult | Exception | None] = asyncio.Queue()

        self._max_concurrent_local = max_concurrent_local
        self._max_concurrent_remote = max_concurrent_remote

        self._start_callbacks: list[StartCallback] = []
        self._heartbeat_callbacks: list[HeartbeatCallback] = []

    def add_task(self, task: DownloadTask, remote_queue: bool = False) -> None:
        """
        Enqueue a task for execution.

        Use remote_queue=True for tasks that dispatch work externally and poll for results
        (eg Globus transfers). Use the default (False) for tasks that perform direct,
        resource-intensive local work (eg HTTPS file downloads).
        """
        queue = self._remote_queue if remote_queue else self._local_queue
        queue.put_nowait(task)

    async def _worker(self, queue: asyncio.Queue[DownloadTask]) -> None:
        while True:
            task = await queue.get()

            for cb in self._start_callbacks:
                task.on_start(cb)
            for cb in self._heartbeat_callbacks:
                task.on_heartbeat(cb)

            try:
                result = await task.run()
                await self._task_results.put(result)

            except asyncio.CancelledError:
                raise
            except Exception as err:
                logging.exception('An unknown error occurred')
                failure = task.to_fail(str(err))
                await self._task_results.put(failure)
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

        This decouples external systems from the orchestrator's core responsibilities

        NOTE: Callbacks can also be registered at the task level, if they require specific behaviors for a task type
            (like "saving globus task ID to database").
        """
        if cb not in self._start_callbacks:
            self._start_callbacks.append(cb)

    async def run_all(self) -> None:
        n_local = min(self._local_queue.qsize(), self._max_concurrent_local)
        n_remote = min(self._remote_queue.qsize(), self._max_concurrent_remote)

        workers = (
            [asyncio.create_task(self._worker(self._local_queue)) for _ in range(n_local)]
            + [asyncio.create_task(self._worker(self._remote_queue)) for _ in range(n_remote)]
        )

        await asyncio.gather(self._local_queue.join(), self._remote_queue.join())
        self._task_results.put_nowait(None)  # sentinel value to indicate all processed

        for w in workers:
            w.cancel()

    async def iter_results(self) -> AsyncGenerator[TaskResult]:
        """Iterate over completed task results as they become available."""
        while (item := await self._task_results.get()) is not None:
            yield item
