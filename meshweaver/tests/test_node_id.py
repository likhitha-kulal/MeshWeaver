"""
Tests for NodeID generation, XOR distance metric, and conversions.
"""

import unittest
from meshweaver.models import NodeID, NodeInfo


class TestNodeID(unittest.TestCase):

    def test_random_generation(self):
        n1 = NodeID()
        n2 = NodeID()
        self.assertEqual(len(n1.bytes), 20)
        self.assertEqual(len(n1.hex()), 40)
        self.assertNotEqual(n1, n2)

    def test_xor_distance_properties(self):
        n1 = NodeID()
        n2 = NodeID()
        n3 = NodeID()

        # Identity: d(x, x) == 0
        self.assertEqual(n1.distance(n1), 0)

        # Symmetry: d(x, y) == d(y, x)
        self.assertEqual(n1.distance(n2), n2.distance(n1))

        # Triangle inequality: d(x, z) <= d(x, y) ^ d(y, z)
        dist_13 = n1.distance(n3)
        dist_12 = n1.distance(n2)
        dist_23 = n2.distance(n3)
        self.assertEqual(dist_13, dist_12 ^ dist_23)

    def test_hex_and_hash_construction(self):
        n1 = NodeID()
        hex_str = n1.hex()
        n2 = NodeID(hex_str)
        self.assertEqual(n1, n2)

        # Hash generation
        nh1 = NodeID.from_string_hash("127.0.0.1:9000")
        nh2 = NodeID.from_string_hash("127.0.0.1:9000")
        self.assertEqual(nh1, nh2)

    def test_node_info_equality_and_serialization(self):
        nid = NodeID()
        info = NodeInfo(node_id=nid, ip="127.0.0.1", udp_port=9000, tcp_port=9001)
        
        # Serialization roundtrip
        info_dict = info.to_dict()
        restored = NodeInfo.from_dict(info_dict)
        self.assertEqual(info, restored)
        self.assertEqual(info, nid)


if __name__ == "__main__":
    unittest.main()
