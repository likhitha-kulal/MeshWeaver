"""
MeshWeaver Week 1 Test Suite
Tests NodeID XOR distances, cloudpickle task serialization, UDP Ping-Pong RPCs,
and TCP remote task execution with error handling.
"""

import asyncio
import os
import sys
import unittest

# Ensure workspace root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.gossip import GossipManager
from meshweaver.models import MessageType, NodeID, TaskResult
from meshweaver.node import MeshNode
from meshweaver.task_serializer import RemoteExecutionError, TaskSerializer


# Sample Functions for Testing
def add(a: int, b: int) -> int:
    return a + b


def fibonacci(n: int) -> int:
    if n <= 0:
        return 0
    elif n == 1:
        return 1
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b


def divide(a: float, b: float) -> float:
    return a / b


async def async_multiplier(a: int, b: int) -> int:
    await asyncio.sleep(0.01)
    return a * b


class TestWeek1(unittest.IsolatedAsyncioTestCase):

    def test_node_id_creation_and_distance(self):
        """Test NodeID hex generation, parsing, and Kademlia XOR distance metric."""
        n1 = NodeID()
        n2 = NodeID()

        self.assertEqual(len(n1.hex()), 40)
        self.assertEqual(len(n2.hex()), 40)

        # Distance to self should be 0
        self.assertEqual(n1.distance(n1), 0)

        # Symmetry: d(a, b) == d(b, a)
        self.assertEqual(n1.distance(n2), n2.distance(n1))

        # Reconstruct from hex
        n1_reconstructed = NodeID(n1.hex())
        self.assertEqual(n1, n1_reconstructed)

    def test_task_serialization_sync_and_async(self):
        """Test serializing and local execution of sync and async functions."""
        # 1. Sync addition
        payload_add = TaskSerializer.serialize(add, 15, 25)
        func, args, kwargs = TaskSerializer.deserialize(payload_add)
        self.assertEqual(func(*args, **kwargs), 40)

        # 2. Closure / Lambda function
        factor = 10
        multiply_closure = lambda x: x * factor
        payload_closure = TaskSerializer.serialize(multiply_closure, 5)
        f_closure, c_args, c_kwargs = TaskSerializer.deserialize(payload_closure)
        self.assertEqual(f_closure(*c_args, **c_kwargs), 50)

    async def test_task_serializer_execute_task_success(self):
        """Test TaskSerializer.execute_task for sync and coroutine functions."""
        payload = TaskSerializer.serialize(async_multiplier, 6, 7)
        task_result = await TaskSerializer.execute_task(payload)

        self.assertTrue(task_result.success)
        result_val = TaskSerializer.unpack_result(task_result)
        self.assertEqual(result_val, 42)

    async def test_task_serializer_execute_task_failure(self):
        """Test TaskSerializer.execute_task when function raises an exception."""
        payload = TaskSerializer.serialize(divide, 10, 0)
        task_result = await TaskSerializer.execute_task(payload)

        self.assertFalse(task_result.success)
        self.assertEqual(task_result.error_type, "ZeroDivisionError")
        self.assertIn("division by zero", task_result.error_message)

        with self.assertRaises(RemoteExecutionError) as ctx:
            TaskSerializer.unpack_result(task_result)

        self.assertEqual(ctx.exception.error_type, "ZeroDivisionError")

    async def test_udp_ping_pong(self):
        """Test UDP PING / PONG RPC discovery between two local nodes."""
        node_a = MeshNode(host="127.0.0.1", udp_port=19000, tcp_port=19001)
        node_b = MeshNode(host="127.0.0.1", udp_port=19002, tcp_port=19003)

        await node_a.start()
        await node_b.start()

        try:
            # Node A pings Node B
            pong_msg = await node_a.ping("127.0.0.1", node_b.bound_udp_port, timeout=3.0)

            self.assertEqual(pong_msg.type, MessageType.PONG)
            self.assertEqual(pong_msg.sender_id, node_b.node_id.hex())
            self.assertEqual(pong_msg.payload.get("status"), "OK")

        finally:
            await node_a.stop()
            await node_b.stop()

    async def test_remote_task_execution(self):
        """Test remote task execution over TCP between Node A and Node B."""
        node_a = MeshNode(host="127.0.0.1", udp_port=19100, tcp_port=19101)
        node_b = MeshNode(host="127.0.0.1", udp_port=19102, tcp_port=19103)

        await node_a.start()
        await node_b.start()

        try:
            # 1. Execute Fibonacci on Node B from Node A
            fib_result = await node_a.submit_task(
                "127.0.0.1", node_b.bound_tcp_port, fibonacci, 10
            )
            self.assertEqual(fib_result, 55)

            # 2. Execute Addition on Node B from Node A
            add_result = await node_a.submit_task(
                "127.0.0.1", node_b.bound_tcp_port, add, 123, 456
            )
            self.assertEqual(add_result, 579)

            # 3. Submit failing task (division by zero)
            with self.assertRaises(RemoteExecutionError) as ctx:
                await node_a.submit_task(
                    "127.0.0.1", node_b.bound_tcp_port, divide, 5, 0
                )

            self.assertEqual(ctx.exception.error_type, "ZeroDivisionError")

        finally:
            await node_a.stop()
            await node_b.stop()

    def test_task_result_payload_hash_and_verification(self):
        """TaskResult payload hashes should detect tampering."""
        result = TaskResult(task_id="t-1", success=True, result_bytes=b"hello world")
        self.assertIsNotNone(result.payload_hash)
        self.assertTrue(result.verify_payload())

        result.result_bytes = b"tampered"
        self.assertFalse(result.verify_payload())

    def test_gossip_manager_tracks_peer_load_and_dead_nodes(self):
        """Gossip manager should store peer load snapshots and evict stale members."""
        manager = GossipManager(node_id="node-a", heartbeat_interval=0.01, dead_node_timeout=0.1)
        manager.register_neighbor("node-b", "127.0.0.1", 9001)

        manager.receive_heartbeat({
            "sender_id": "node-b",
            "ip": "127.0.0.1",
            "udp_port": 9001,
            "cpu_percent": 42.0,
            "ram_percent": 55.0,
            "timestamp": 1000.0,
        })

        self.assertAlmostEqual(manager.peer_loads["node-b"].cpu_percent, 42.0)
        manager.peer_loads["node-b"].timestamp = 0.0
        evicted = manager.expire_dead_nodes(now=1000.2)
        self.assertIn("node-b", evicted)
        self.assertNotIn("node-b", manager.peer_loads)


if __name__ == "__main__":
    unittest.main()
