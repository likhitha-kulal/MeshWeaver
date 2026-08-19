"""
Tests for KBucket contact list management and LRU eviction.
"""

import unittest
from meshweaver.kbucket import KBucket
from meshweaver.models import NodeID, NodeInfo


class TestKBucket(unittest.TestCase):

    def test_add_and_lru_ordering(self):
        bucket = KBucket(k=3)
        c1 = NodeInfo(NodeID(), "127.0.0.1", 9001)
        c2 = NodeInfo(NodeID(), "127.0.0.1", 9002)
        c3 = NodeInfo(NodeID(), "127.0.0.1", 9003)

        self.assertTrue(bucket.add(c1))
        self.assertTrue(bucket.add(c2))
        self.assertTrue(bucket.add(c3))
        self.assertEqual(len(bucket), 3)
        self.assertTrue(bucket.is_full())

        self.assertEqual(bucket.head(), c1)
        self.assertEqual(bucket.tail(), c3)

        # Refresh c1 -> becomes most recently seen
        self.assertTrue(bucket.add(c1))
        self.assertEqual(bucket.head(), c2)
        self.assertEqual(bucket.tail(), c1)

    def test_replacement_cache_and_promotion(self):
        bucket = KBucket(k=2)
        c1 = NodeInfo(NodeID(), "127.0.0.1", 9001)
        c2 = NodeInfo(NodeID(), "127.0.0.1", 9002)
        c3 = NodeInfo(NodeID(), "127.0.0.1", 9003)

        bucket.add(c1)
        bucket.add(c2)

        # Bucket is full -> c3 placed in replacement cache
        self.assertFalse(bucket.add(c3))
        self.assertEqual(len(bucket), 2)
        self.assertEqual(len(bucket.replacement_cache), 1)

        # Evict c1 -> c3 automatically promoted
        removed = bucket.remove(c1.node_id)
        self.assertEqual(removed, c1)
        self.assertEqual(len(bucket), 2)
        self.assertIn(c3.node_id, bucket)
        self.assertEqual(len(bucket.replacement_cache), 0)


if __name__ == "__main__":
    unittest.main()
