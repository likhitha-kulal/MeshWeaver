"""
Unit tests for DistributedBarrier, DistributedCountdownLatch, DistributedSemaphore,
and SynchronizationManager.
"""

import asyncio
import time
import unittest

from meshweaver.barrier import (
    DistributedBarrier,
    DistributedCountdownLatch,
    DistributedSemaphore,
    SynchronizationManager,
)
from meshweaver.models import BarrierState


class TestDistributedSynchronization(unittest.IsolatedAsyncioTestCase):
    async def test_distributed_barrier_rendezvous(self):
        barrier = DistributedBarrier("sync_stage_1", threshold=3, timeout_seconds=2.0)
        results = []

        async def worker(pid: str):
            res = await barrier.enter(pid)
            results.append((pid, res))

        # Launch 3 workers concurrently
        tasks = [
            asyncio.create_task(worker("w1")),
            asyncio.create_task(worker("w2")),
            asyncio.create_task(worker("w3")),
        ]
        await asyncio.gather(*tasks)

        self.assertEqual(len(results), 3)
        self.assertTrue(all(r[1] for r in results))
        self.assertEqual(barrier.generation, 1)
        self.assertEqual(barrier.state, BarrierState.RELEASED)

    async def test_distributed_barrier_timeout(self):
        barrier = DistributedBarrier("timeout_barrier", threshold=3, timeout_seconds=0.1)

        # Only 2 workers enter -> should timeout
        t1 = asyncio.create_task(barrier.enter("p1"))
        t2 = asyncio.create_task(barrier.enter("p2"))

        res1 = await t1
        res2 = await t2

        self.assertFalse(res1)
        self.assertFalse(res2)
        self.assertEqual(barrier.state, BarrierState.TIMED_OUT)

    async def test_distributed_countdown_latch(self):
        latch = DistributedCountdownLatch("pipeline_latch", count=3)
        self.assertFalse(latch.is_zero())

        completed = []

        async def waiter():
            ok = await latch.wait(timeout=2.0)
            completed.append(ok)

        wait_task = asyncio.create_task(waiter())

        await latch.count_down(1)
        self.assertEqual(latch.current_count, 2)
        self.assertEqual(len(completed), 0)

        await latch.count_down(2)
        self.assertEqual(latch.current_count, 0)
        self.assertTrue(latch.is_zero())

        await wait_task
        self.assertEqual(completed, [True])

    async def test_distributed_semaphore_concurrency_and_leases(self):
        sem = DistributedSemaphore("gpu_pool", total_permits=2, default_ttl_seconds=0.2)
        self.assertEqual(sem.available_permits, 2)

        # Acquire 2 permits
        ok1 = await sem.acquire("task_1", ttl_seconds=1.0)
        ok2 = await sem.acquire("task_2", ttl_seconds=1.0)
        self.assertTrue(ok1)
        self.assertTrue(ok2)
        self.assertEqual(sem.available_permits, 0)

        # Third acquire should block until release
        acquired_3 = []

        async def third_worker():
            ok3 = await sem.acquire("task_3", timeout=1.0)
            acquired_3.append(ok3)

        task3 = asyncio.create_task(third_worker())
        await asyncio.sleep(0.05)
        self.assertEqual(len(acquired_3), 0)

        # Release task_1 -> task_3 should be granted immediately
        await sem.release("task_1")
        await task3

        self.assertEqual(acquired_3, [True])
        self.assertEqual(sem.active_permits, 2)

        # Test lease expiration
        sem_exp = DistributedSemaphore("short_lease", total_permits=1, default_ttl_seconds=0.1)
        await sem_exp.acquire("short_holder", ttl_seconds=0.08)
        self.assertEqual(sem_exp.available_permits, 0)
        await asyncio.sleep(0.12)
        # Expired -> permit should be reclaimed
        reclaimed = sem_exp.cleanup_expired_leases()
        self.assertEqual(reclaimed, 1)
        self.assertEqual(sem_exp.available_permits, 1)

    async def test_synchronization_manager_registry(self):
        sm = SynchronizationManager("node-sync-test")
        b = sm.get_or_create_barrier("b1", threshold=2)
        l = sm.get_or_create_latch("l1", count=1)
        s = sm.get_or_create_semaphore("s1", total_permits=3)

        self.assertIsNotNone(b)
        self.assertIsNotNone(l)
        self.assertIsNotNone(s)

        b_specs = sm.get_all_barrier_specs()
        s_specs = sm.get_all_semaphore_specs()
        self.assertEqual(len(b_specs), 1)
        self.assertEqual(len(s_specs), 1)
        self.assertEqual(b_specs[0].barrier_id, "b1")
        self.assertEqual(s_specs[0].semaphore_id, "s1")


if __name__ == "__main__":
    unittest.main()
