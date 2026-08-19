"""
MeshWeaver Week 2 Test Suite
Tests gossip protocol, peer discovery, task integrity hashing, and corrupted payload handling.
"""

import asyncio
import os
import sys
import time
import unittest

# Ensure workspace root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from meshweaver.gossip import GossipManager, PeerLoadSnapshot
from meshweaver.models import TaskEnvelope, TaskResult
from meshweaver.networking import TCPTaskClient, TCPTaskServer
from meshweaver.node import MeshNode
from meshweaver.task_serializer import TaskSerializer, RemoteExecutionError


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


class TestWeek2Gossip(unittest.IsolatedAsyncioTestCase):

    # ===== Test 1: Two GossipManagers gossiping to each other =====
    async def test_two_gossip_managers_peer_discovery(self):
        """
        Two GossipService instances gossiping to each other → 
        each ends up with the other in peer_loads within ~2 intervals.
        """
        # Create two nodes with specific ports
        node1 = MeshNode(host="127.0.0.1", udp_port=9100)
        node2 = MeshNode(host="127.0.0.1", udp_port=9110)

        try:
            # Start both nodes
            await node1.start()
            await node2.start()

            # Register node2 as a neighbor of node1
            node1.register_neighbor(
                node_id=node2.node_id.hex(),
                host="127.0.0.1",
                udp_port=node2.bound_udp_port,
                tcp_port=node2.bound_tcp_port,
            )

            # Register node1 as a neighbor of node2
            node2.register_neighbor(
                node_id=node1.node_id.hex(),
                host="127.0.0.1",
                udp_port=node1.bound_udp_port,
                tcp_port=node1.bound_tcp_port,
            )

            # Wait for ~2 heartbeat intervals (default interval is 5s, so 10s for 2 intervals)
            await asyncio.sleep(12)

            # Check that node1 knows about node2
            node2_in_node1_table = node2.node_id.hex() in node1.gossip_manager.peer_loads
            self.assertTrue(node2_in_node1_table, "Node2 should be in Node1's peer_loads after gossip")

            # Check that node2 knows about node1
            node1_in_node2_table = node1.node_id.hex() in node2.gossip_manager.peer_loads
            self.assertTrue(node1_in_node2_table, "Node1 should be in Node2's peer_loads after gossip")

        finally:
            await node1.stop()
            await node2.stop()

    # ===== Test 2: Dead node eviction =====
    async def test_dead_node_eviction_after_timeout(self):
        """
        A node that stops broadcasting gets evicted after dead_timeout.
        """
        # Create two nodes with a shorter dead_node_timeout for testing (5 seconds)
        node1 = MeshNode(host="127.0.0.1", udp_port=9120)
        node2 = MeshNode(host="127.0.0.1", udp_port=9130)

        # Override the dead_node_timeout for testing
        node1.gossip_manager.dead_node_timeout = 5.0
        node2.gossip_manager.dead_node_timeout = 5.0

        try:
            # Start both nodes
            await node1.start()
            await node2.start()

            # Register each as neighbor of the other
            node1.register_neighbor(
                node_id=node2.node_id.hex(),
                host="127.0.0.1",
                udp_port=node2.bound_udp_port,
                tcp_port=node2.bound_tcp_port,
            )

            node2.register_neighbor(
                node_id=node1.node_id.hex(),
                host="127.0.0.1",
                udp_port=node1.bound_udp_port,
                tcp_port=node1.bound_tcp_port,
            )

            # Wait for initial peer discovery
            await asyncio.sleep(8)
            self.assertIn(node2.node_id.hex(), node1.gossip_manager.peer_loads)

            # Stop node2 broadcasting
            await node2.gossip_manager.stop()

            # Wait for timeout + some buffer
            await asyncio.sleep(6)

            # Node2 should be evicted from node1's peer_loads
            self.assertNotIn(node2.node_id.hex(), node1.gossip_manager.peer_loads,
                            "Dead node should be evicted after timeout")

        finally:
            await node1.stop()
            await node2.stop()

    # ===== Test 3: get_least_loaded_peer() selection =====
    async def test_get_least_loaded_peer_selection(self):
        """
        get_least_loaded_peer() picks correctly given a table with 3+ synthetic entries.
        """
        manager = GossipManager(
            node_id="test_node_001",
            host="127.0.0.1",
            udp_port=9140,
            heartbeat_interval=5.0,
            dead_node_timeout=15.0,
        )

        # Manually add 3+ synthetic peer snapshots with different loads
        manager.peer_loads["peer_1"] = PeerLoadSnapshot(
            node_id="peer_1", ip="127.0.0.1", udp_port=9141,
            cpu_percent=50.0, ram_percent=60.0
        )
        manager.peer_loads["peer_2"] = PeerLoadSnapshot(
            node_id="peer_2", ip="127.0.0.1", udp_port=9142,
            cpu_percent=20.0, ram_percent=30.0  # Least loaded
        )
        manager.peer_loads["peer_3"] = PeerLoadSnapshot(
            node_id="peer_3", ip="127.0.0.1", udp_port=9143,
            cpu_percent=80.0, ram_percent=85.0
        )
        manager.peer_loads["peer_4"] = PeerLoadSnapshot(
            node_id="peer_4", ip="127.0.0.1", udp_port=9144,
            cpu_percent=40.0, ram_percent=50.0
        )

        # Get least loaded peer
        least_loaded = manager.get_least_loaded_peer()

        # Should select peer_2 (lowest load)
        self.assertIsNotNone(least_loaded)
        self.assertEqual(least_loaded.node_id, "peer_2",
                        "Should select peer with lowest combined load")

    # ===== Test 4: TaskEnvelope integrity verification =====
    async def test_task_envelope_verify_with_flipped_byte(self):
        """
        TaskEnvelope.verify() returns False after flipping a byte in payload.
        """
        # Create a simple payload
        payload = b"test task payload data"

        # Wrap it in an envelope
        envelope = TaskEnvelope.wrap(payload)

        # Verify initially passes
        self.assertTrue(envelope.verify(), "Initial verification should pass")

        # Flip a byte in the payload
        corrupted_payload = bytearray(envelope.payload)
        corrupted_payload[5] ^= 0xFF  # Flip all bits in byte at index 5
        envelope.payload = bytes(corrupted_payload)

        # Verify now fails
        self.assertFalse(envelope.verify(), "Verification should fail after payload corruption")

    # ===== Test 5: TaskEnvelope JSON serialization round-trip =====
    async def test_task_envelope_json_round_trip(self):
        """
        TaskEnvelope can be serialized and deserialized via JSON.
        """
        payload = b"another test payload"
        envelope = TaskEnvelope.wrap(payload)

        # Serialize to dict and back
        env_dict = envelope.to_dict()
        restored_envelope = TaskEnvelope.from_dict(env_dict)

        # Should match original
        self.assertEqual(restored_envelope.payload, envelope.payload)
        self.assertEqual(restored_envelope.sha256, envelope.sha256)
        self.assertTrue(restored_envelope.verify())

    # ===== Test 6: End-to-end corrupted payload handling =====
    async def test_corrupted_payload_tcp_integrity_error(self):
        """
        End-to-end: corrupted payload sent over TCP → 
        server responds with IntegrityError, never calls cloudpickle.loads.
        """
        # Create a server node
        server_node = MeshNode(host="127.0.0.1", udp_port=9150, tcp_port=9160)

        try:
            await server_node.start()

            # Serialize a legitimate task
            payload_bytes = TaskSerializer.serialize(add, 10, 20)

            # Parse the envelope and corrupt the hash (simulating tampering)
            import json
            envelope_dict = json.loads(payload_bytes.decode("utf-8"))
            # Flip the hash to make it invalid
            original_hash = envelope_dict["sha256"]
            corrupted_hash = "0" * 64  # Wrong hash
            envelope_dict["sha256"] = corrupted_hash
            corrupted_payload = json.dumps(envelope_dict).encode("utf-8")

            # Send corrupted payload to server and expect IntegrityError
            result = await TCPTaskClient.send_task(
                "127.0.0.1",
                server_node.bound_tcp_port,
                corrupted_payload,
                timeout=5.0
            )

            # Should receive an IntegrityError result, NOT a successful execution
            self.assertFalse(result.success, "Server should reject corrupted payload")
            self.assertEqual(result.error_type, "IntegrityError",
                           "Error should be IntegrityError for hash mismatch")
            self.assertIn("hash verification failed", result.error_message.lower(),
                         "Error message should reference hash verification")

        finally:
            await server_node.stop()

    # ===== Test 7: Valid task execution (integrity passes) =====
    async def test_valid_task_execution_with_envelope(self):
        """
        Valid task with correct envelope hash executes successfully.
        """
        server_node = MeshNode(host="127.0.0.1", udp_port=9170, tcp_port=9180)

        try:
            await server_node.start()

            # Serialize a task (which wraps it in TaskEnvelope)
            payload_bytes = TaskSerializer.serialize(add, 15, 25)

            # Send to server (envelope hash is correct)
            result = await TCPTaskClient.send_task(
                "127.0.0.1",
                server_node.bound_tcp_port,
                payload_bytes,
                timeout=5.0
            )

            # Should succeed
            self.assertTrue(result.success, "Valid task should execute successfully")

            # Unpack the result
            output = TaskSerializer.unpack_result(result)
            self.assertEqual(output, 40, "Task result should be 15 + 25 = 40")

        finally:
            await server_node.stop()

    # ===== Test 8: TaskEnvelope in serialization roundtrip =====
    async def test_task_serialization_with_envelope_roundtrip(self):
        """
        Serialization wraps payload in TaskEnvelope and deserialize unwraps it.
        """
        # Serialize a task (wraps in envelope)
        payload_bytes = TaskSerializer.serialize(fibonacci, 10)

        # This should be JSON of TaskEnvelope
        import json
        envelope_dict = json.loads(payload_bytes.decode("utf-8"))
        self.assertIn("payload", envelope_dict)
        self.assertIn("sha256", envelope_dict)

        # Deserialize should work (and verify hash)
        func, args, kwargs = TaskSerializer.deserialize(payload_bytes)

        self.assertEqual(func, fibonacci)
        self.assertEqual(args, (10,))
        self.assertEqual(kwargs, {})

    # ===== Test 9: Integrity check prevents deserialization of tampered data =====
    async def test_integrity_check_prevents_tampered_deserialization(self):
        """
        Even if someone tampering data in transit, deserialize detects it.
        """
        payload_bytes = TaskSerializer.serialize(add, 5, 10)

        # Tamper with the payload (corrupt the hash)
        import json
        envelope_dict = json.loads(payload_bytes.decode("utf-8"))
        envelope_dict["sha256"] = "0" * 64  # Wrong hash

        tampered_bytes = json.dumps(envelope_dict).encode("utf-8")

        # Deserialization should fail with ValueError mentioning hash mismatch
        with self.assertRaises(ValueError) as ctx:
            TaskSerializer.deserialize(tampered_bytes)

        self.assertIn("hash mismatch", str(ctx.exception).lower())

    # ===== Test 10: GossipManager peer_table initialization =====
    async def test_gossip_manager_peer_table_initialization(self):
        """
        GossipManager initializes with empty peer_table.
        """
        manager = GossipManager(
            node_id="test_node_999",
            host="127.0.0.1",
            udp_port=9190,
        )

        self.assertEqual(len(manager.peer_table), 0, "Peer table should start empty")

    # ===== Test 11: Register and retrieve neighbor =====
    async def test_gossip_manager_register_neighbor(self):
        """
        Can register and retrieve a neighbor in GossipManager.
        """
        manager = GossipManager(
            node_id="test_node_aaa",
            host="127.0.0.1",
            udp_port=9200,
        )

        # Manually add a peer snapshot
        peer_id = "neighbor_node_111"
        manager.peer_table[peer_id] = PeerLoadSnapshot(
            node_id=peer_id,
            ip="192.168.1.100",
            udp_port=9999,
            cpu_percent=25.0,
            ram_percent=40.0
        )

        # Retrieve and verify
        peer = manager.peer_table.get(peer_id)
        self.assertIsNotNone(peer)
        self.assertEqual(peer.node_id, peer_id)
        self.assertEqual(peer.ip, "192.168.1.100")

    # ===== Test 12: Least loaded peer with tie-breaking =====
    async def test_get_least_loaded_peer_tie_breaking(self):
        """
        When multiple peers have same load, should pick one consistently.
        """
        manager = GossipManager(
            node_id="test_node_bbb",
            host="127.0.0.1",
            udp_port=9210,
        )

        # Add peers with equal loads
        for i in range(3):
            manager.peer_table[f"peer_equal_{i}"] = PeerLoadSnapshot(
                node_id=f"peer_equal_{i}",
                ip="127.0.0.1",
                udp_port=9220 + i,
                cpu_percent=50.0,
                ram_percent=50.0,
            )

        # Should return one of them
        least_loaded = manager.get_least_loaded_peer()
        self.assertIsNotNone(least_loaded)
        self.assertIn("peer_equal", least_loaded.node_id)


if __name__ == "__main__":
    unittest.main()
