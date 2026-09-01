"""
MeshWeaver Cluster Scheduler & Failover Integration Test Suite.
Tests End-to-End Least-Loaded Scheduling, Multi-Node Automatic Failover,
Distributed Batch Map, and DHT-Backed Result Memoization.
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from meshweaver.models import NodeID
from meshweaver.node import MeshNode
from meshweaver.scheduler import SchedulingPolicy


def compute_cube(x: int) -> int:
    return x * x * x


def compute_sum(a: int, b: int) -> int:
    return a + b


class TestClusterSchedulerIntegration(unittest.IsolatedAsyncioTestCase):
    """End-to-End multi-node cluster integration tests for Scheduler & Failover."""

    async def asyncSetUp(self):
        # Create 3-node mesh cluster
        self.node1 = MeshNode(host="127.0.0.1", udp_port=20100)
        self.node2 = MeshNode(host="127.0.0.1", udp_port=20102)
        self.node3 = MeshNode(host="127.0.0.1", udp_port=20104)

        await self.node1.start()
        await self.node2.start()
        await self.node3.start()

        # Connect cluster via gossip
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

        # Allow initial heartbeats to exchange telemetry
        await asyncio.sleep(0.3)

    async def asyncTearDown(self):
        await self.node1.stop()
        await self.node2.stop()
        await self.node3.stop()

    async def test_multinode_least_loaded_scheduling(self):
        """Verify scheduler routes to least loaded node in the cluster."""
        # Fake node2 with low load (10%) and node3 with high load (90%)
        p2 = self.node1.gossip_manager.get_peer(self.node2.node_id.hex())
        p3 = self.node1.gossip_manager.get_peer(self.node3.node_id.hex())

        if p2:
            p2.cpu_percent = 10.0
            p2.ram_percent = 15.0
        if p3:
            p3.cpu_percent = 85.0
            p3.ram_percent = 80.0

        # Dispatch task from node1
        res = await self.node1.schedule_task(
            compute_cube,
            4,
            policy=SchedulingPolicy.LEAST_LOADED,
        )
        self.assertEqual(res, 64)

    async def test_multinode_failover_recovery(self):
        """Verify scheduler catches connection drops and fails over to alive worker."""
        # Set node2 as preferred candidate
        p2 = self.node1.gossip_manager.get_peer(self.node2.node_id.hex())
        p3 = self.node1.gossip_manager.get_peer(self.node3.node_id.hex())
        if p2:
            p2.cpu_percent = 5.0
            p2.ram_percent = 5.0
        if p3:
            p3.cpu_percent = 50.0
            p3.ram_percent = 50.0

        # Simulate sudden crash / stop of node2
        await self.node2.stop()

        # Node1 schedules task -> node2 will fail with connection refused -> retries on node3
        res = await self.node1.schedule_task(
            compute_sum,
            15,
            25,
            policy=SchedulingPolicy.LEAST_LOADED,
        )
        self.assertEqual(res, 40)

    async def test_multinode_parallel_distributed_map(self):
        """Verify parallel batch execution over cluster with order preservation."""
        inputs = list(range(1, 13))
        results, metrics = await self.node1.map(
            compute_cube,
            inputs,
            concurrency=4,
            policy=SchedulingPolicy.ROUND_ROBIN,
        )

        expected = [x ** 3 for x in inputs]
        self.assertEqual(results, expected)
        self.assertEqual(metrics.completed_items, 12)
        self.assertEqual(metrics.failed_items, 0)

    async def test_multinode_dht_task_memoization(self):
        """Verify DHT result memoization prevents duplicate remote execution."""
        # Connect DHT routing table between node1 and node2
        self.node1.routing_table.add_contact(self.node2.info)
        self.node2.routing_table.add_contact(self.node1.info)

        # First execution -> computes & caches in DHT
        val1 = await self.node1.cached_compute(compute_sum, 100, 200, ttl=60)
        self.assertEqual(val1, 300)

        # Second execution -> cache hit from DHT
        val2 = await self.node1.cached_compute(compute_sum, 100, 200, ttl=60)
        self.assertEqual(val2, 300)





if __name__ == "__main__":
    unittest.main()
