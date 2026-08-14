"""
MeshWeaver Week 1 Test Suite (Networking & Infra Track - Person A)
Tests NodeID XOR distances, hex string parsing, and UDP Ping-Pong RPCs.
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from meshweaver.models import MessageType, NodeID
from meshweaver.node import MeshNode


class TestWeek1Networking(unittest.IsolatedAsyncioTestCase):

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

    async def test_udp_ping_pong(self):
        """Test UDP PING / PONG RPC discovery between two local nodes."""
        node_a = MeshNode(host="127.0.0.1", udp_port=19000)
        node_b = MeshNode(host="127.0.0.1", udp_port=19002)

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


if __name__ == "__main__":
    unittest.main()
