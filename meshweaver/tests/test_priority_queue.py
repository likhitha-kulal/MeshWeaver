"""
Unit tests for MeshWeaver PriorityTaskQueue, Starvation-Free Aging, and QoS tiers.
"""

import asyncio
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from meshweaver.priority_queue import (
    PriorityDispatcher,
    PriorityTaskQueue,
    PrioritizedTask,
    TaskPriority,
    calculate_deadline_urgency,
)



class TestPriorityQueue(unittest.IsolatedAsyncioTestCase):
    """Test suite for priority queue mechanics and QoS dynamic aging."""

    def test_priority_enum_values(self):
        """Verify priority levels have correct precedence (lower value = higher priority)."""
        self.assertLess(TaskPriority.CRITICAL.value, TaskPriority.HIGH.value)
        self.assertLess(TaskPriority.HIGH.value, TaskPriority.NORMAL.value)
        self.assertLess(TaskPriority.NORMAL.value, TaskPriority.LOW.value)
        self.assertLess(TaskPriority.LOW.value, TaskPriority.BACKGROUND.value)

    async def test_priority_ordering(self):
        """Verify tasks are popped strictly in priority order."""
        pq = PriorityTaskQueue(aging_interval_seconds=100.0)  # disable fast aging

        now = time.time()
        t_low = PrioritizedTask(task_id="t_low", func=lambda: 1, base_priority=TaskPriority.LOW, created_at=now)
        t_crit = PrioritizedTask(task_id="t_crit", func=lambda: 2, base_priority=TaskPriority.CRITICAL, created_at=now)
        t_norm = PrioritizedTask(task_id="t_norm", func=lambda: 3, base_priority=TaskPriority.NORMAL, created_at=now)
        t_high = PrioritizedTask(task_id="t_high", func=lambda: 4, base_priority=TaskPriority.HIGH, created_at=now)

        await pq.push(t_low)
        await pq.push(t_crit)
        await pq.push(t_norm)
        await pq.push(t_high)

        self.assertEqual(pq.qsize(), 4)

        p1 = await pq.pop()
        p2 = await pq.pop()
        p3 = await pq.pop()
        p4 = await pq.pop()

        self.assertEqual(p1.task_id, "t_crit")
        self.assertEqual(p2.task_id, "t_high")
        self.assertEqual(p3.task_id, "t_norm")
        self.assertEqual(p4.task_id, "t_low")
        self.assertTrue(pq.is_empty())

    async def test_fifo_tie_breaking(self):
        """Verify tasks with same priority are resolved FIFO using sequence numbers."""
        pq = PriorityTaskQueue(aging_interval_seconds=100.0)

        now = time.time()
        t1 = PrioritizedTask(task_id="t1", func=lambda: 1, base_priority=TaskPriority.NORMAL, created_at=now)
        t2 = PrioritizedTask(task_id="t2", func=lambda: 2, base_priority=TaskPriority.NORMAL, created_at=now)
        t3 = PrioritizedTask(task_id="t3", func=lambda: 3, base_priority=TaskPriority.NORMAL, created_at=now)

        await pq.push(t1)
        await pq.push(t2)
        await pq.push(t3)

        self.assertEqual((await pq.pop()).task_id, "t1")
        self.assertEqual((await pq.pop()).task_id, "t2")
        self.assertEqual((await pq.pop()).task_id, "t3")

    async def test_aging_promotion_starvation_prevention(self):
        """Verify that older low-priority tasks get promoted over newer high-priority tasks."""
        # aging_interval = 1.0s -> after 3s, LOW (3) gains 3 levels = 0 (CRITICAL level)
        pq = PriorityTaskQueue(aging_interval_seconds=1.0)

        old_time = time.time() - 4.0  # waited 4 seconds
        t_old_low = PrioritizedTask(
            task_id="t_old_low",
            func=lambda: "old",
            base_priority=TaskPriority.LOW,
            created_at=old_time,
        )

        now = time.time()
        t_new_high = PrioritizedTask(
            task_id="t_new_high",
            func=lambda: "new",
            base_priority=TaskPriority.HIGH,
            created_at=now,
        )

        await pq.push(t_new_high)
        await pq.push(t_old_low)

        # Aged low task should pop first because 3.0 - (4.0/1.0) = -1.0 < 1.0 (High)
        popped = await pq.pop()
        self.assertEqual(popped.task_id, "t_old_low")

    def test_deadline_urgency_calculation(self):
        """Test deadline calculation helper."""
        now = 100.0
        # No deadline
        self.assertEqual(calculate_deadline_urgency(None, current_time=now), 0.0)
        # Far deadline (10s away > 5s threshold)
        self.assertEqual(calculate_deadline_urgency(110.0, current_time=now), 0.0)
        # Urgent deadline (2.5s away -> 50% boost of 2.0 = 1.0)
        boost = calculate_deadline_urgency(102.5, current_time=now, deadline_boost_weight=2.0)
        self.assertAlmostEqual(boost, 1.0)
        # Expired deadline -> 2 * max_boost = 4.0
        boost_expired = calculate_deadline_urgency(99.0, current_time=now, deadline_boost_weight=2.0)
        self.assertEqual(boost_expired, 4.0)

    async def test_queue_snapshot_and_metrics(self):
        """Test queue snapshot listing and telemetry metrics."""
        pq = PriorityTaskQueue(aging_interval_seconds=2.0)

        t1 = PrioritizedTask(task_id="task_snap_1", func=lambda: 1, base_priority=TaskPriority.HIGH)
        t2 = PrioritizedTask(task_id="task_snap_2", func=lambda: 2, base_priority=TaskPriority.LOW)

        await pq.push(t1)
        await pq.push(t2)

        snapshot = pq.get_effective_queue_snapshot()
        self.assertEqual(len(snapshot), 2)
        self.assertEqual(snapshot[0]["task_id"], "task_snap_1")
        self.assertEqual(snapshot[0]["base_priority"], "HIGH")

        self.assertEqual(pq.metrics.total_enqueued, 2)
        self.assertEqual(pq.metrics.tasks_by_priority[TaskPriority.HIGH.value], 1)
        self.assertEqual(pq.metrics.tasks_by_priority[TaskPriority.LOW.value], 1)

    async def test_priority_dispatcher_execution_and_preemption(self):
        """Verify PriorityDispatcher executes CRITICAL task ahead of pending LOW tasks."""
        class MockScheduler:
            def __init__(self):
                self.execution_order = []

            async def dispatch_task(self, func, *args, **kwargs):
                val = func(*args, **kwargs)
                self.execution_order.append(val)
                await asyncio.sleep(0.02)
                return val

        mock_sched = MockScheduler()
        dispatcher = PriorityDispatcher(scheduler=mock_sched, concurrency=1)
        # Disable fast aging during preemption test
        dispatcher.queue.aging_interval_seconds = 100.0

        await dispatcher.start()

        # Submit 3 LOW tasks and then 1 CRITICAL task
        f_low1 = await dispatcher.submit(lambda: "low_1", priority=TaskPriority.LOW)
        f_low2 = await dispatcher.submit(lambda: "low_2", priority=TaskPriority.LOW)
        f_low3 = await dispatcher.submit(lambda: "low_3", priority=TaskPriority.LOW)
        f_crit = await dispatcher.submit(lambda: "critical_urgent", priority=TaskPriority.CRITICAL)

        res_crit = await f_crit
        res_low1 = await f_low1
        res_low2 = await f_low2
        res_low3 = await f_low3

        self.assertEqual(res_crit, "critical_urgent")
        self.assertEqual(res_low1, "low_1")

        # Critical task jumped ahead of all low tasks
        self.assertEqual(mock_sched.execution_order[0], "critical_urgent")
        self.assertIn("low_1", mock_sched.execution_order[1:])
        self.assertIn("low_2", mock_sched.execution_order[1:])
        self.assertIn("low_3", mock_sched.execution_order[1:])


        # Check stats
        stats = dispatcher.get_stats()
        self.assertEqual(stats["total_enqueued"], 4)
        self.assertEqual(stats["total_completed"], 4)

        await dispatcher.stop()

    async def test_priority_dispatcher_cancellation_and_flush(self):
        """Test task cancellation and queue flushing in PriorityDispatcher."""
        dispatcher = PriorityDispatcher(scheduler=None, concurrency=1)
        dispatcher.pause()  # prevent workers from popping
        await dispatcher.start()

        fut1 = await dispatcher.submit(lambda: 10, priority=TaskPriority.NORMAL, task_id="to_cancel")
        fut2 = await dispatcher.submit(lambda: 20, priority=TaskPriority.LOW, task_id="to_flush")

        # Cancel specific task
        cancelled = dispatcher.cancel_task("to_cancel")
        self.assertTrue(cancelled)
        self.assertTrue(fut1.cancelled())

        # Flush remainder
        flushed = dispatcher.flush()
        self.assertEqual(flushed, 1)
        self.assertTrue(fut2.cancelled())
        self.assertEqual(dispatcher.queue.qsize(), 0)

        await dispatcher.stop()


if __name__ == "__main__":
    unittest.main()

