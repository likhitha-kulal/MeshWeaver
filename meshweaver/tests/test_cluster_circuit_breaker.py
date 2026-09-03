"""
MeshWeaver Cluster Circuit Breaker Integration Test Suite.
Tests multi-node circuit breaker tripping upon worker node failures,
automated candidate exclusion, half-open recovery probing, and cluster resilience.
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from meshweaver.circuit_breaker import CircuitBreakerConfig, CircuitState
from meshweaver.node import MeshNode
from meshweaver.scheduler import RetryPolicy, SchedulingPolicy


def compute_square(x: int) -> int:
    return x * x


def compute_add_ten(x: int) -> int:
    return x + 10


class TestClusterCircuitBreakerIntegration(unittest.IsolatedAsyncioTestCase):
    """End-to-End multi-node cluster integration tests for Circuit Breaker."""

    async def asyncSetUp(self):
        cb_cfg = CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=0.1,
            half_open_success_threshold=2,
        )

        self.node1 = MeshNode(host="127.0.0.1", udp_port=20200, circuit_breaker_config=cb_cfg)
        self.node2 = MeshNode(host="127.0.0.1", udp_port=20202, circuit_breaker_config=cb_cfg)
        self.node3 = MeshNode(host="127.0.0.1", udp_port=20204, circuit_breaker_config=cb_cfg)

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

        await asyncio.sleep(0.3)

    async def asyncTearDown(self):
        await self.node1.stop()
        await self.node2.stop()
        await self.node3.stop()

    async def test_cluster_circuit_breaker_tripping_and_routing(self):
        """
        Verify circuit breaker trips OPEN on consecutive remote failures,
        and scheduler bypasses the tripped node immediately.
        """
        node2_id = self.node2.node_id.hex()
        node3_id = self.node3.node_id.hex()

        # Configure node2 as lowest load candidate
        p2 = self.node1.gossip_manager.get_peer(node2_id)
        p3 = self.node1.gossip_manager.get_peer(node3_id)
        if p2:
            p2.cpu_percent = 5.0
            p2.ram_percent = 5.0
        if p3:
            p3.cpu_percent = 50.0
            p3.ram_percent = 50.0

        # Verify initial state: circuit CLOSED
        cb_node2 = self.node1.circuit_breakers.get_or_create(node2_id)
        self.assertEqual(cb_node2.state, CircuitState.CLOSED)

        # Stop node2 to simulate crash
        await self.node2.stop()

        retry_policy = RetryPolicy(max_retries=2, backoff_factor=0.01)

        # First dispatch will fail on node2 and failover to node3
        res1 = await self.node1.schedule_task(
            compute_square,
            7,
            policy=SchedulingPolicy.LEAST_LOADED,
            retry_policy=retry_policy,
        )
        self.assertEqual(res1, 49)
        self.assertEqual(cb_node2.failure_count, 1)
        self.assertEqual(cb_node2.state, CircuitState.CLOSED)

        # Second dispatch fails on node2 again and fails over to node3
        res2 = await self.node1.schedule_task(
            compute_square,
            8,
            policy=SchedulingPolicy.LEAST_LOADED,
            retry_policy=retry_policy,
        )
        self.assertEqual(res2, 64)
        # Threshold reached (2 failures) -> circuit trips OPEN!
        self.assertEqual(cb_node2.state, CircuitState.OPEN)
        self.assertIn(node2_id, self.node1.get_tripped_nodes())

        # Third dispatch: scheduler candidates filter out node2 completely without attempting TCP connection!
        candidates = self.node1.scheduler.get_active_candidates()
        candidate_ids = [c.node_id for c in candidates]
        self.assertNotIn(node2_id, candidate_ids)
        self.assertIn(node3_id, candidate_ids)

        res3 = await self.node1.schedule_task(
            compute_add_ten,
            20,
            policy=SchedulingPolicy.LEAST_LOADED,
        )
        self.assertEqual(res3, 30)

    async def test_cluster_circuit_recovery_to_half_open_and_closed(self):
        """
        Verify circuit transitions from OPEN to HALF_OPEN after recovery_timeout,
        and closes upon successful probe tasks when worker is restarted.
        """
        node2_id = self.node2.node_id.hex()
        cb_node2 = self.node1.circuit_breakers.get_or_create(node2_id)

        # Trip circuit for node2
        cb_node2.record_failure()
        cb_node2.record_failure()
        self.assertEqual(cb_node2.state, CircuitState.OPEN)

        # Wait for recovery timeout (0.1s)
        await asyncio.sleep(0.12)
        self.assertEqual(cb_node2.state, CircuitState.HALF_OPEN)
        self.assertTrue(cb_node2.is_available())

        # Successful probe 1
        res1 = await self.node1.schedule_task(compute_square, 5)
        self.assertEqual(res1, 25)

        # Successful probe 2
        res2 = await self.node1.schedule_task(compute_square, 6)
        self.assertEqual(res2, 36)

        # Circuit should now be closed
        self.assertEqual(cb_node2.state, CircuitState.CLOSED)
        self.assertEqual(cb_node2.failure_count, 0)

    async def test_node_health_summary_and_stale_breaker_cleanup(self):
        """Verify health summary generation and stale breaker cleanup."""
        summary = self.node1.get_node_health_summary()
        self.assertEqual(summary["total_known_peers"], 2)
        self.assertEqual(summary["healthy_peers"], 2)
        self.assertEqual(len(summary["tripped_nodes"]), 0)

        # Register a fake ghost breaker
        self.node1.circuit_breakers.get_or_create("ghost-node-999")
        self.assertEqual(self.node1.circuit_breakers.registered_count, 1)

        # Stale cleanup should remove ghost node because it's not in active gossip peers
        cleaned = self.node1.cleanup_stale_breakers()
        self.assertEqual(cleaned, 1)
        self.assertEqual(self.node1.circuit_breakers.registered_count, 0)
