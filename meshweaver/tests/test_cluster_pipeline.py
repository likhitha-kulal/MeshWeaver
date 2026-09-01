"""
MeshWeaver MapReduce & Pipeline Cluster Integration Test Suite.
Tests End-to-End Multi-Node Distributed MapReduce and Multi-Stage Task Pipelines
over real TCP and UDP socket connections.
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from meshweaver.models import NodeID
from meshweaver.node import MeshNode
from meshweaver.scheduler import SchedulingPolicy


def text_chunk_mapper(chunk: str):
    words = chunk.lower().split()
    return [(w.strip(".,!"), 1) for w in words if w.strip(".,!")]


def count_reducer(word: str, counts: list) -> int:
    return sum(counts)


def multiply_by_three(x: int) -> int:
    return x * 3


def subtract_two(x: int) -> int:
    return x - 2


def list_product(items: list) -> int:
    prod = 1
    for el in items:
        prod *= el
    return prod


class TestClusterPipelineIntegration(unittest.IsolatedAsyncioTestCase):
    """End-to-End cluster integration tests for MapReduce and Task Pipelines."""

    async def asyncSetUp(self):
        # Create 3-node mesh cluster
        self.node1 = MeshNode(host="127.0.0.1", udp_port=20300)
        self.node2 = MeshNode(host="127.0.0.1", udp_port=20302)
        self.node3 = MeshNode(host="127.0.0.1", udp_port=20304)

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

    async def test_cluster_distributed_map_reduce(self):
        """Verify distributed MapReduce word counting across cluster nodes."""
        documents = [
            "MeshWeaver is a decentralized peer to peer compute mesh",
            "Distributed computing enables scalable parallel batch tasks",
            "MeshWeaver handles automatic failover across cluster nodes",
            "Decentralized peer discovery via Kademlia DHT routing table",
        ]

        word_counts, metrics = await self.node1.map_reduce(
            map_fn=text_chunk_mapper,
            reduce_fn=count_reducer,
            data=documents,
            chunk_size=1,
            policy=SchedulingPolicy.LEAST_LOADED,
        )

        self.assertEqual(word_counts["meshweaver"], 2)
        self.assertEqual(word_counts["decentralized"], 2)
        self.assertEqual(word_counts["peer"], 3)
        self.assertGreater(metrics.total_intermediate_pairs, 20)
        self.assertGreater(metrics.throughput_items_per_sec, 0)
        self.assertTrue(metrics.map_duration_seconds >= 0)
        self.assertTrue(metrics.reduce_duration_seconds >= 0)

    async def test_cluster_task_pipeline_execution(self):
        """Verify multi-stage pipeline dispatch across cluster workers."""
        pipeline = self.node1.create_pipeline()
        pipeline.pipe("Multiply by 3", multiply_by_three, is_parallel=True)
        pipeline.pipe("Subtract 2", subtract_two, is_parallel=True)
        pipeline.add_stage("Product Reducer", list_product, is_parallel=False)

        # Input: [2, 3, 4]
        # Stage 1: [6, 9, 12]
        # Stage 2: [4, 7, 10]
        # Stage 3: 4 * 7 * 10 = 280
        result, metrics = await pipeline.execute([2, 3, 4])

        self.assertEqual(result, 280)
        self.assertEqual(metrics.total_stages, 3)
        self.assertTrue(metrics.is_successful)


if __name__ == "__main__":
    unittest.main()
