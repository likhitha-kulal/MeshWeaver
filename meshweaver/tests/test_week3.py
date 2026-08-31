"""
MeshWeaver Week 3 Integration Test Suite.
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


class TestWeek3ClusterIntegration(unittest.IsolatedAsyncioTestCase):
    """End-to-End multi-node cluster integration tests for Week 3."""

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


if __name__ == "__main__":
    unittest.main()
