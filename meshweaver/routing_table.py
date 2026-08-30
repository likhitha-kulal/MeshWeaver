"""
MeshWeaver Routing Table
160-bit Kademlia routing table for distance-based contact routing and nearest-neighbor lookups.
"""

import logging
from typing import List, Optional

from meshweaver.kbucket import KBucket
from meshweaver.models import NodeID, NodeInfo

logger = logging.getLogger("meshweaver.routing_table")


class RoutingTable:
    """
    Routing table partitioning 160-bit keyspace into 160 distance buckets.
    """

    NUM_BUCKETS = 160

    def __init__(self, node_id: NodeID, k: int = 20):
        self.node_id = node_id
        self.k = k
        self.buckets: List[KBucket] = [KBucket(k=k) for _ in range(self.NUM_BUCKETS)]

    def get_bucket_index(self, node_id: NodeID) -> int:
        """Calculate bucket index based on floor(log2(distance))."""
        dist = self.node_id.distance(node_id)
        if dist == 0:
            return 0
        bit_len = dist.bit_length()
        return min(bit_len - 1, self.NUM_BUCKETS - 1)

    def add_contact(self, node_info: NodeInfo) -> bool:
        """
        Add or refresh a contact in the appropriate distance bucket.
        Ignores self node identifier.
        """
        if node_info.node_id == self.node_id:
            return False

        bucket_index = self.get_bucket_index(node_info.node_id)
        bucket = self.buckets[bucket_index]
        return bucket.add(node_info)

    def remove_contact(self, node_id: NodeID) -> Optional[NodeInfo]:
        """Remove contact by NodeID from corresponding bucket."""
        if node_id == self.node_id:
            return None
        bucket_index = self.get_bucket_index(node_id)
        return self.buckets[bucket_index].remove(node_id)

    def get_contact(self, node_id: NodeID) -> Optional[NodeInfo]:
        """Retrieve contact from routing table if known."""
        if node_id == self.node_id:
            return None
        bucket_index = self.get_bucket_index(node_id)
        return self.buckets[bucket_index].get(node_id)

    def find_closest_nodes(
        self,
        target_id: NodeID,
        count: Optional[int] = None,
        exclude: Optional[NodeID] = None,
    ) -> List[NodeInfo]:
        """
        Find up to `count` contacts in routing table closest to `target_id`
        ordered by XOR distance metric.
        """
        limit = count if count is not None else self.k
        all_contacts: List[NodeInfo] = []

        for bucket in self.buckets:
            for contact in bucket.nodes:
                if exclude and contact.node_id == exclude:
                    continue
                all_contacts.append(contact)

        all_contacts.sort(key=lambda contact: contact.node_id.distance(target_id))
        return all_contacts[:limit]

    def get_all_contacts(self) -> List[NodeInfo]:
        """Return all active contacts across all buckets."""
        contacts: List[NodeInfo] = []
        for bucket in self.buckets:
            contacts.extend(bucket.nodes)
        return contacts

    def total_contacts(self) -> int:
        """Return total count of contacts across all buckets."""
        return sum(len(bucket) for bucket in self.buckets)

    def __repr__(self) -> str:
        return f"<RoutingTable node_id={self.node_id.hex()[:8]}... total_contacts={self.total_contacts()}>"
