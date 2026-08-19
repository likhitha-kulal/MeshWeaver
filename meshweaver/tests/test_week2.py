"""
MeshWeaver Week 2 Test Suite (Networking & DHT Routing Track - Person A)
Tests KBucket operations, RoutingTable distance indexing & closest-node queries,
UDP FIND_NODE RPCs, and Multi-Node Bootstrapping.
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from meshweaver.kbucket import KBucket
from meshweaver.models import MessageType, NodeID, NodeInfo
from meshweaver.node import MeshNode
from meshweaver.routing_table import RoutingTable


class TestKBucket(unittest.TestCase):
    """Test suite for KBucket data structure."""

    def test_kbucket_add_and_lru_order(self):
        bucket = KBucket(k=3)
        c1 = NodeInfo(NodeID(), "127.0.0.1", 9001)
        c2 = NodeInfo(NodeID(), "127.0.0.1", 9002)
        c3 = NodeInfo(NodeID(), "127.0.0.1", 9003)

        self.assertTrue(bucket.add(c1))
        self.assertTrue(bucket.add(c2))
        self.assertTrue(bucket.add(c3))
        self.assertEqual(len(bucket), 3)
        self.assertTrue(bucket.is_full())

        # Check LRU head and MRU tail
        self.assertEqual(bucket.head(), c1)
        self.assertEqual(bucket.tail(), c3)

        # Updating c1 moves it to tail (MRU)
        self.assertTrue(bucket.add(c1))
        self.assertEqual(bucket.head(), c2)
        self.assertEqual(bucket.tail(), c1)

    def test_kbucket_replacement_cache_and_promotion(self):
        bucket = KBucket(k=2)
        c1 = NodeInfo(NodeID(), "127.0.0.1", 9001)
        c2 = NodeInfo(NodeID(), "127.0.0.1", 9002)
        c3 = NodeInfo(NodeID(), "127.0.0.1", 9003)

        bucket.add(c1)
        bucket.add(c2)

        # Bucket is full, c3 goes to replacement cache
        added = bucket.add(c3)
        self.assertFalse(added)
        self.assertEqual(len(bucket), 2)
        self.assertEqual(len(bucket.replacement_cache), 1)

        # Remove c1 -> c3 should be promoted automatically
        removed = bucket.remove(c1.node_id)
        self.assertEqual(removed, c1)
        self.assertEqual(len(bucket), 2)
        self.assertIn(c3.node_id, bucket)
        self.assertEqual(len(bucket.replacement_cache), 0)


class TestRoutingTable(unittest.TestCase):
    """Test suite for 160-bit Kademlia Routing Table."""

    def setUp(self):
        self.local_id = NodeID()
        self.table = RoutingTable(self.local_id, k=20)

    def test_bucket_index_calculation(self):
        # Distance to self gives index 0
        self.assertEqual(self.table.get_bucket_index(self.local_id), 0)

        # Distance with bit length 160 gives index 159
        target = NodeID(self.local_id.int ^ (1 << 159))
        self.assertEqual(self.table.get_bucket_index(target), 159)

        # Distance with bit length 1 gives index 0
        target_close = NodeID(self.local_id.int ^ 1)
        self.assertEqual(self.table.get_bucket_index(target_close), 0)

    def test_prevent_adding_self(self):
        self_info = NodeInfo(self.local_id, "127.0.0.1", 9000)
        self.assertFalse(self.table.add_contact(self_info))
        self.assertEqual(self.table.total_contacts(), 0)

    def test_find_closest_nodes_ordering(self):
        # Create a set of contacts with distinct known distances
        contacts = []
        target_id = NodeID()

        for _ in range(30):
            node_info = NodeInfo(NodeID(), "127.0.0.1", 9000)
            contacts.append(node_info)
            self.table.add_contact(node_info)

        closest = self.table.find_closest_nodes(target_id, count=10)
        self.assertEqual(len(closest), 10)

        # Verify sorted ascending by XOR distance to target_id
        distances = [c.node_id.distance(target_id) for c in closest]
        self.assertEqual(distances, sorted(distances))


class TestWeek2Networking(unittest.IsolatedAsyncioTestCase):
    """Integration test suite for FIND_NODE RPC and Bootstrapping."""

    async def test_find_node_rpc(self):
        node_a = MeshNode(host="127.0.0.1", udp_port=19200, tcp_port=19201)
        node_b = MeshNode(host="127.0.0.1", udp_port=19202, tcp_port=19203)

        # Add some known contacts to Node B's routing table
        sample_peer1 = NodeInfo(NodeID(), "127.0.0.1", 19204)
        sample_peer2 = NodeInfo(NodeID(), "127.0.0.1", 19206)
        node_b.routing_table.add_contact(sample_peer1)
        node_b.routing_table.add_contact(sample_peer2)

        await node_a.start()
        await node_b.start()

        try:
            target_id = NodeID()
            # Node A queries Node B for nodes closest to target_id
            discovered_nodes = await node_a.find_node(
                "127.0.0.1",
                node_b.bound_udp_port,
                target_id,
                timeout=3.0,
            )

            self.assertGreaterEqual(len(discovered_nodes), 2)
            discovered_ids = {n.node_id.hex() for n in discovered_nodes}
            self.assertIn(sample_peer1.node_id.hex(), discovered_ids)
            self.assertIn(sample_peer2.node_id.hex(), discovered_ids)

            # Node A should also have added Node B to its routing table automatically
            self.assertIn(node_b.node_id, node_a.routing_table.get_all_contacts())

        finally:
            await node_a.stop()
            await node_b.stop()

    async def test_bootstrap_sequence(self):
        # Cluster: Bootstrap Node (A), Existing Node (B), Joining Node (C)
        node_a = MeshNode(host="127.0.0.1", udp_port=19300, tcp_port=19301)
        node_b = MeshNode(host="127.0.0.1", udp_port=19302, tcp_port=19303)
        node_c = MeshNode(host="127.0.0.1", udp_port=19304, tcp_port=19305)

        await node_a.start()
        await node_b.start()
        await node_c.start()

        try:
            # Node B pings Node A (so Node A knows Node B)
            await node_b.ping("127.0.0.1", node_a.bound_udp_port)

            # Node C bootstraps via Node A
            discovered_count = await node_c.bootstrap(
                [("127.0.0.1", node_a.bound_udp_port)],
                timeout=3.0,
            )

            # Node C should have discovered Node A and Node B
            known_ids = {c.node_id.hex() for c in node_c.routing_table.get_all_contacts()}
            self.assertIn(node_a.node_id.hex(), known_ids)
            self.assertIn(node_b.node_id.hex(), known_ids)

        finally:
            await node_a.stop()
            await node_b.stop()
            await node_c.stop()


if __name__ == "__main__":
    unittest.main()
