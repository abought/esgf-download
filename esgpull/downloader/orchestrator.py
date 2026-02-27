import asyncio
from typing import AsyncGenerator, Callable

from esgpull.downloader.base import DownloadTask, TaskResult, TaskStartInfo, StartCallback


class Orchestrator:
    """
    Orchestrate a series of file download tasks
    """
    def __init__(self, tasks: asyncio.Queue[DownloadTask], max_concurrent: int=5):
        self._tasks = tasks
        self._task_results: asyncio.Queue[TaskResult | Exception | None] = asyncio.Queue()

        self._max_concurrent = max_concurrent

        self._start_callbacks: list[StartCallback] = []

    def _run_start_callbacks(self, start_info: TaskStartInfo) -> None:
        """
        Start callbacks can be defined at both the pipeline level (generic tracking) and the task level
            (specific behaviors for a given task type, like "save globus task ID to database")
        """
        # TODO: Heartbeat events have been moved into the task; should start events live there or in the orchestrator?
        #  (we may want different start event types per task type, which this orchestrator interface does not support)
        for cb in self._start_callbacks:
            cb(start_info)

    async def _worker(self) -> None:
        while True:
            task = await self._tasks.get()

            try:
                start_info = await task.start()
                self._run_start_callbacks(start_info)

                # TODO: Consider whether to allow certain tasks to exempt themselves from live mode, eg let esgpull
                #   process shut down while globus transfers are still running
                result = await task.result()
                await self._task_results.put(result)

            except asyncio.CancelledError:
                raise
            except Exception as err:
                await self._task_results.put(err)
            finally:
                self._tasks.task_done()

    #### Public interface
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
        """Iterate over the completed tasks"""
        while (item := await self._task_results.get()) is not None:
            yield item
