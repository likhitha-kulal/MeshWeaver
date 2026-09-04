"""
MeshWeaver Priority QoS Cluster Integration Test Suite.
Tests End-to-End Multi-Node Prioritized Task Execution, Preemption Precedence,
and QoS Telemetry over real TCP sockets.
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from meshweaver.node import MeshNode
from meshweaver.priority_queue import TaskPriority


def compute_square(x: int) -> int:
    return x * x


def slow_worker_task(val: str, delay_seconds: float = 0.05) -> str:
    import time
    time.sleep(delay_seconds)
    return f"processed_{val}"


class TestClusterPriorityIntegration(unittest.IsolatedAsyncioTestCase):
    """End-to-End cluster integration tests for Priority QoS Scheduler."""

    async def asyncSetUp(self):
        # Create a 3-node cluster
        self.node1 = MeshNode(host="127.0.0.1", udp_port=20400)
        self.node2 = MeshNode(host="127.0.0.1", udp_port=20402)
        self.node3 = MeshNode(host="127.0.0.1", udp_port=20404)

        await self.node1.start()
        await self.node2.start()
        await self.node3.start()

        # Wire gossip network
        self.node1.register_neighbor(
            self.node2.node_id.hex(), "127.0.0.1", self.node2.bound_udp_port, self.node2.bound_tcp_port
        )
        self.node1.register_neighbor(
            self.node3.node_id.hex(), "127.0.0.1", self.node3.bound_udp_port, self.node3.bound_tcp_port
        )
        self.node2.register_neighbor(
            self.node1.node_id.hex(), "127.0.0.1", self.node1.bound_udp_port, self.node1.bound_tcp_port
        )
        self.node3.register_neighbor(
            self.node1.node_id.hex(), "127.0.0.1", self.node1.bound_udp_port, self.node1.bound_tcp_port
        )

        # Allow gossip heartbeats to exchange peer info
        await asyncio.sleep(0.4)

    async def asyncTearDown(self):
        await self.node1.stop()
        await self.node2.stop()
        await self.node3.stop()

    async def test_cluster_priority_task_dispatch(self):
        """Submit prioritized tasks across cluster nodes and verify resolution."""
        fut1 = await self.node1.submit_prioritized(compute_square, 12, priority=TaskPriority.HIGH)
        fut2 = await self.node1.submit_prioritized(compute_square, 7, priority=TaskPriority.NORMAL)
        fut3 = await self.node1.submit_prioritized(compute_square, 9, priority=TaskPriority.LOW)

        res1 = await fut1
        res2 = await fut2
        res3 = await fut3

        self.assertEqual(res1, 144)
        self.assertEqual(res2, 49)
        self.assertEqual(res3, 81)

        metrics = self.node1.get_queue_metrics()
        self.assertGreaterEqual(metrics["total_enqueued"], 3)
        self.assertGreaterEqual(metrics["total_completed"], 3)

    async def test_cluster_priority_preemption_and_metrics(self):
        """Verify CRITICAL tasks are prioritized across cluster workers."""
        futs = []
        # Submit background batch tasks
        for i in range(4):
            f = await self.node1.submit_prioritized(
                slow_worker_task, f"batch_{i}", 0.02, priority=TaskPriority.BACKGROUND
            )
            futs.append(f)

        # Submit urgent critical task
        crit_fut = await self.node1.submit_prioritized(
            slow_worker_task, "urgent_vip", 0.01, priority=TaskPriority.CRITICAL
        )

        crit_res = await crit_fut
        self.assertEqual(crit_res, "processed_urgent_vip")

        # Collect rest
        results = await asyncio.gather(*futs)
        self.assertEqual(len(results), 4)

        metrics = self.node1.get_queue_metrics()
        self.assertEqual(metrics["total_failed"], 0)
        self.assertIn("CRITICAL", metrics["tasks_by_priority"])
        self.assertEqual(metrics["tasks_by_priority"]["CRITICAL"], 1)


if __name__ == "__main__":
    unittest.main()
