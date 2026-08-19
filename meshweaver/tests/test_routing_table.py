"""
Tests for Kademlia Routing Table indexing and nearest-neighbor lookups.
"""

import unittest
from meshweaver.models import NodeID, NodeInfo
from meshweaver.routing_table import RoutingTable


class TestRoutingTable(unittest.TestCase):

    def setUp(self):
        self.local_id = NodeID()
        self.table = RoutingTable(self.local_id, k=20)

    def test_bucket_index_partitioning(self):
        self.assertEqual(self.table.get_bucket_index(self.local_id), 0)

        # Extreme distance: highest bit set -> bucket 159
        target = NodeID(self.local_id.int ^ (1 << 159))
        self.assertEqual(self.table.get_bucket_index(target), 159)

        # Close distance: lowest bit set -> bucket 0
        target_close = NodeID(self.local_id.int ^ 1)
        self.assertEqual(self.table.get_bucket_index(target_close), 0)

    def test_ignore_self(self):
        self_info = NodeInfo(self.local_id, "127.0.0.1", 9000)
        self.assertFalse(self.table.add_contact(self_info))
        self.assertEqual(self.table.total_contacts(), 0)

    def test_find_closest_nodes(self):
        contacts = []
        target_id = NodeID()

        for _ in range(40):
            node_info = NodeInfo(NodeID(), "127.0.0.1", 9000)
            contacts.append(node_info)
            self.table.add_contact(node_info)

        closest = self.table.find_closest_nodes(target_id, count=10)
        self.assertEqual(len(closest), 10)

        # Distances must be strictly monotonically increasing
        distances = [c.node_id.distance(target_id) for c in closest]
        self.assertEqual(distances, sorted(distances))


if __name__ == "__main__":
    unittest.main()
