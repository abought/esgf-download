import asyncio
import logging
from typing import AsyncGenerator

from esgpull.downloader.base import DownloadTask, TaskResult, StartCallback, HeartbeatCallback


class Orchestrator:
    """
    Orchestrate a series of file download tasks
    """
    def __init__(self, tasks: asyncio.Queue[DownloadTask], max_concurrent: int=5):
        self._tasks = tasks
        self._task_results: asyncio.Queue[TaskResult | Exception | None] = asyncio.Queue()

        # TODO: Consider a separate and higher limit for globus transfers, because they put much less stress on the system
        #   (or consider a polling time that is longer for more transfers at once)
        self._max_concurrent = max_concurrent

        self._start_callbacks: list[StartCallback] = []
        self._heartbeat_callbacks: list[HeartbeatCallback] = []

    async def _worker(self) -> None:
        while True:
            task = await self._tasks.get()

            # Register orchestrator-level start callbacks on the task so they fire
            # at the right point within task.run() (after submission, before polling)
            for cb in self._start_callbacks:
                task.on_start(cb)
            for cb in self._heartbeat_callbacks:
                task.on_heartbeat(cb)

            try:
                # TODO: Consider whether to allow certain tasks to exempt themselves from live mode, eg let esgpull
                #   process shut down while globus transfers are still running
                result = await task.run()
                await self._task_results.put(result)

            except asyncio.CancelledError:
                raise
            except Exception as err:
                logging.exception('An unknown error occurred')
                failure = task.to_fail(str(err))
                await self._task_results.put(failure)
            finally:
                self._tasks.task_done()

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

    async def run_all(self):
        n_workers = min(self._tasks.qsize(), self._max_concurrent)
        workers = [
            asyncio.create_task(self._worker()) for _ in range(n_workers)
        ]

        await self._tasks.join()
        self._task_results.put_nowait(None)  # sentinel value to indicate all processed

        for w in workers:
            w.cancel()

    async def iter_results(self) -> AsyncGenerator[TaskResult | Exception]:
        """Iterate over the completed tasks, as they become available"""
        while (item := await self._task_results.get()) is not None:
            yield item
