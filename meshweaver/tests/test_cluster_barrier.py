"""
Integration test for multi-node cluster distributed barrier rendezvous and synchronization.
"""

import asyncio
import unittest

from meshweaver.models import BarrierState
from meshweaver.node import MeshNode


class TestClusterBarrierIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_multi_node_distributed_barrier_and_semaphore(self):
        n1 = MeshNode(host="127.0.0.1", udp_port=19900, tcp_port=19901)
        n2 = MeshNode(host="127.0.0.1", udp_port=19910, tcp_port=19911)
        n3 = MeshNode(host="127.0.0.1", udp_port=19920, tcp_port=19921)

        nodes = [n1, n2, n3]
        for n in nodes:
            await n.start()

        try:
            # Bootstrap cluster
            await n2.bootstrap([("127.0.0.1", 19900)])
            await n3.bootstrap([("127.0.0.1", 19900)])
            await asyncio.sleep(0.15)

            # 1. Distributed Rendezvous Barrier
            barrier_id = "cluster_mapreduce_barrier_1"
            b1 = n1.create_barrier(barrier_id, threshold=3, timeout_seconds=5.0)

            results = []

            async def node_worker(node: MeshNode, pid: str):
                ok = await b1.enter(pid, timeout=4.0)
                results.append((node.node_id.hex()[:8], ok))

            tasks = [
                asyncio.create_task(node_worker(n1, "worker_1")),
                asyncio.create_task(node_worker(n2, "worker_2")),
                asyncio.create_task(node_worker(n3, "worker_3")),
            ]
            await asyncio.gather(*tasks)

            self.assertEqual(len(results), 3)
            self.assertTrue(all(r[1] for r in results))
            self.assertEqual(b1.state, BarrierState.RELEASED)
            self.assertEqual(b1.generation, 1)

            # 2. Distributed Semaphore concurrency
            sem_ok1 = await n1.acquire_semaphore("gpu_pool", total_permits=2, ttl_seconds=5.0)
            sem_ok2 = await n2.acquire_semaphore("gpu_pool", total_permits=2, ttl_seconds=5.0)
            self.assertTrue(sem_ok1)
            self.assertTrue(sem_ok2)

            # Release
            rel_ok = await n1.release_semaphore("gpu_pool")
            self.assertTrue(rel_ok)

            # 3. Distributed Countdown Latch
            latch = n1.create_countdown_latch("stage_completion", count=2)
            await latch.count_down(1)
            self.assertEqual(latch.current_count, 1)
            await latch.count_down(1)
            self.assertTrue(latch.is_zero())

            # Verify sync telemetry
            sync_metrics = n1.get_synchronization_metrics()
            self.assertEqual(len(sync_metrics["barriers"]), 1)
            self.assertEqual(len(sync_metrics["semaphores"]), 1)

        finally:
            for n in nodes:
                try:
                    await n.stop()
                except Exception:
                    pass


if __name__ == "__main__":
    unittest.main()
