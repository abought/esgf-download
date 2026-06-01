"""Unit tests for Orchestrator."""
import asyncio
import contextlib
from unittest.mock import MagicMock

from esgpull.downloader.base import TaskResultEvent, TaskStatus
from esgpull.downloader.orchestrator import Orchestrator
from tests.downloader.fakes import FakeDownloadTask, FakeHeartbeatTask, make_file


async def collect(orch: Orchestrator) -> list[TaskResultEvent]:
    return [r async for r in orch.iter_results()]


class TestBasicExecution:
    def test_single_local_task_yields_one_result(self):
        orch = Orchestrator()
        orch.add_local_task(FakeDownloadTask("t", [make_file()]))
        assert len(asyncio.run(collect(orch))) == 1

    def test_single_remote_task_yields_one_result(self):
        orch = Orchestrator()
        orch.add_remote_task(FakeDownloadTask("t", [make_file()]))
        assert len(asyncio.run(collect(orch))) == 1

    def test_n_local_tasks_yield_n_results(self):
        orch = Orchestrator()
        for i in range(4):
            orch.add_local_task(FakeDownloadTask(f"t{i}", [make_file(file_id=f"f{i}")]))
        assert len(asyncio.run(collect(orch))) == 4

    def test_empty_queue_yields_nothing(self):
        assert asyncio.run(collect(Orchestrator())) == []

    def test_task_label_present_in_result(self):
        orch = Orchestrator()
        orch.add_local_task(FakeDownloadTask("my-task", [make_file()]))
        results = asyncio.run(collect(orch))
        assert results[0].task_label == "my-task"


class TestMixedQueues:
    def test_local_and_remote_results_both_yielded(self):
        orch = Orchestrator()
        orch.add_local_task(FakeDownloadTask("local", [make_file(file_id="l")]))
        orch.add_remote_task(FakeDownloadTask("remote", [make_file(file_id="r")]))
        results = asyncio.run(collect(orch))
        assert {r.task_label for r in results} == {"local", "remote"}

    def test_result_count_is_sum_of_both_queues(self):
        orch = Orchestrator()
        for i in range(3):
            orch.add_local_task(FakeDownloadTask(f"l{i}", [make_file(file_id=f"lf{i}")]))
        for i in range(2):
            orch.add_remote_task(FakeDownloadTask(f"r{i}", [make_file(file_id=f"rf{i}")]))
        assert len(asyncio.run(collect(orch))) == 5


class TestCallbackPropagation:
    def test_on_task_start_fires_for_each_task(self):
        orch = Orchestrator()
        labels = []
        orch.on_task_start(lambda e: labels.append(e.task_label))
        for i in range(3):
            orch.add_local_task(FakeDownloadTask(f"task-{i}", [make_file(file_id=f"f{i}")]))
        asyncio.run(collect(orch))
        assert set(labels) == {"task-0", "task-1", "task-2"}

    def test_on_heartbeat_fires_for_each_heartbeat_emitting_task(self):
        orch = Orchestrator()
        heartbeats = []
        orch.on_heartbeat(lambda e: heartbeats.append(e.task_label))
        for i in range(2):
            orch.add_local_task(FakeHeartbeatTask(f"task-{i}", [make_file(file_id=f"f{i}")]))
        asyncio.run(collect(orch))
        assert len(heartbeats) == 2

    def test_task_level_and_orchestrator_start_callbacks_both_fire(self):
        """start callbacks registered at task level and orchestrator level both fire."""
        orch_log, task_log = [], []
        orch = Orchestrator()
        orch.on_task_start(lambda e: orch_log.append(e.task_label))

        task = FakeDownloadTask("t", [make_file()])
        task.on_start(lambda e: task_log.append(e.task_label))
        orch.add_local_task(task)

        asyncio.run(collect(orch))
        assert orch_log == ["t"]
        assert task_log == ["t"]

    def test_task_level_and_orchestrator_heartbeat_callbacks_both_fire(self):
        """heartbeat callbacks registered at task level and orchestrator level both fire."""
        orch_beats, task_beats = [], []
        orch = Orchestrator()
        orch.on_heartbeat(lambda e: orch_beats.append(e.task_label))

        task = FakeHeartbeatTask("t", [make_file()])
        task.on_heartbeat(lambda e: task_beats.append(e.task_label))
        orch.add_local_task(task)

        asyncio.run(collect(orch))
        assert orch_beats == ["t"]
        assert task_beats == ["t"]

    def test_same_start_callback_at_both_levels_fires_once(self):
        """Even if a callback is (mistakenly) attached in two different ways, it should only fire once"""
        cb = MagicMock()
        orch = Orchestrator()
        orch.on_task_start(cb)

        task = FakeDownloadTask("t", [make_file()])
        task.on_start(cb)  # same object registered before enqueue
        orch.add_local_task(task)

        asyncio.run(collect(orch))
        assert cb.call_count == 1

    def test_same_heartbeat_callback_at_both_levels_fires_once(self):
        """Same dedup contract holds for heartbeat callbacks."""
        cb = MagicMock()
        orch = Orchestrator()
        orch.on_heartbeat(cb)

        task = FakeHeartbeatTask("t", [make_file()])
        task.on_heartbeat(cb)  # same object registered before enqueue
        orch.add_local_task(task)

        asyncio.run(collect(orch))
        assert cb.call_count == 1


class TestWorkerExceptionHandling:
    def test_exception_yields_fail_result(self):
        class FailingTask(FakeDownloadTask):
            async def _run(self, to_download, skip):
                raise RuntimeError("download error")

        orch = Orchestrator()
        orch.add_local_task(FailingTask("t", [make_file()]))
        results = asyncio.run(collect(orch))
        assert len(results) == 1
        assert results[0].status == TaskStatus.FAIL

    def test_exception_message_propagated_to_result(self):
        class FailingTask(FakeDownloadTask):
            async def _run(self, to_download, skip):
                raise RuntimeError("disk full")

        orch = Orchestrator()
        orch.add_local_task(FailingTask("t", [make_file()]))
        results = asyncio.run(collect(orch))
        assert "disk full" in results[0].msg

    def test_exception_does_not_stop_subsequent_tasks(self):
        """Worker continues draining the queue after a task raises Exception."""
        class FailingTask(FakeDownloadTask):
            async def _run(self, to_download, skip):
                raise RuntimeError("oops")

        # max_concurrent_local=1 forces serial execution — predictable ordering
        orch = Orchestrator(max_concurrent_local=1)
        orch.add_local_task(FailingTask("fail", [make_file(file_id="a")]))
        orch.add_local_task(FakeDownloadTask("ok", [make_file(file_id="b")]))
        results = asyncio.run(collect(orch))
        by_label = {r.task_label: r.status for r in results}
        assert by_label["fail"] == TaskStatus.FAIL
        assert by_label["ok"] == TaskStatus.SUCCESS


class TestConcurrency:
    def test_max_concurrent_local_limits_parallel_execution(self):
        async def run():
            concurrent = 0
            peak = 0

            class TrackingTask(FakeDownloadTask):
                async def _run(self, to_download, skip):
                    nonlocal concurrent, peak
                    concurrent += 1
                    peak = max(peak, concurrent)
                    await asyncio.sleep(0)  # yield so both workers can start their tasks
                    concurrent -= 1
                    return self._make_result(TaskStatus.SUCCESS, "ok", [])

            orch = Orchestrator(max_concurrent_local=2)
            for i in range(5):
                orch.add_local_task(TrackingTask(f"t{i}", [make_file(file_id=f"f{i}")]))

            results = await collect(orch)
            assert len(results) == 5
            assert peak <= 2

        asyncio.run(run())

    def test_all_tasks_complete_when_max_less_than_total(self):
        """Tasks in excess of the worker limit are queued and run after earlier ones finish."""
        orch = Orchestrator(max_concurrent_local=2)
        for i in range(6):
            orch.add_local_task(FakeDownloadTask(f"t{i}", [make_file(file_id=f"f{i}")]))
        assert len(asyncio.run(collect(orch))) == 6


class TestCollectCancels:
    def test_unstarted_local_tasks_receive_cancel_result(self):
        async def run():
            orch = Orchestrator()
            orch.add_local_task(FakeDownloadTask("t1", [make_file(file_id="a")]))
            orch.add_local_task(FakeDownloadTask("t2", [make_file(file_id="b")]))
            # Never call iter_results — tasks remain in queue
            return await orch.collect_cancels()

        results = asyncio.run(run())
        assert len(results) == 2
        assert all(r.status == TaskStatus.CANCELED for r in results)

    def test_unstarted_remote_tasks_receive_cancel_result(self):
        async def run():
            orch = Orchestrator()
            orch.add_remote_task(FakeDownloadTask("r1", [make_file()]))
            return await orch.collect_cancels()

        results = asyncio.run(run())
        assert len(results) == 1
        assert results[0].status == TaskStatus.CANCELED

    def test_returns_empty_when_nothing_pending(self):
        assert asyncio.run(Orchestrator().collect_cancels()) == []

    def test_in_flight_task_receives_cancel_result_after_interruption(self):
        async def run():
            started = asyncio.Event()

            class BlockingTask(FakeDownloadTask):
                async def _run(self, to_download, skip):
                    started.set()
                    await asyncio.sleep(3600)  # blocks until cancelled
                    return self._make_result(TaskStatus.SUCCESS, "ok", [])

            orch = Orchestrator()
            orch.add_local_task(BlockingTask("t", [make_file()]))

            consumer = asyncio.create_task(collect(orch))
            await started.wait()  # ensure the task is actually running before we cancel

            consumer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await consumer

            return await orch.collect_cancels()

        results = asyncio.run(run())
        assert len(results) == 1
        assert results[0].status == TaskStatus.CANCELED

    def test_both_in_flight_and_queued_tasks_receive_cancel_result(self):
        """With 1 worker blocked, the second task stays queued; both must appear in cancels."""
        async def run():
            started = asyncio.Event()

            class BlockingTask(FakeDownloadTask):
                async def _run(self, to_download, skip):
                    started.set()
                    await asyncio.sleep(3600)
                    return self._make_result(TaskStatus.SUCCESS, "ok", [])

            orch = Orchestrator(max_concurrent_local=1)
            orch.add_local_task(BlockingTask("in-flight", [make_file(file_id="a")]))
            orch.add_local_task(FakeDownloadTask("queued", [make_file(file_id="b")]))

            consumer = asyncio.create_task(collect(orch))
            await started.wait()

            consumer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await consumer

            return await orch.collect_cancels()

        results = asyncio.run(run())
        assert len(results) == 2
        assert all(r.status == TaskStatus.CANCELED for r in results)

    def test_collect_cancels_safe_to_call_after_normal_completion(self):
        """Calling collect_cancels() after a clean run returns nothing."""
        async def run():
            orch = Orchestrator()
            orch.add_local_task(FakeDownloadTask("t", [make_file()]))
            await collect(orch)
            return await orch.collect_cancels()

        assert asyncio.run(run()) == []