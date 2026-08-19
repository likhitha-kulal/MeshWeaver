"""
MeshWeaver Routing Table Implementation
Maintains 160 K-Buckets indexed by XOR distance for Kademlia DHT routing.
"""

import logging
from typing import List, Optional

from meshweaver.kbucket import KBucket
from meshweaver.models import NodeID, NodeInfo

logger = logging.getLogger("meshweaver.routing_table")


class RoutingTable:
    """
    Kademlia Routing Table containing 160 K-Buckets.
    Manages contact placement based on XOR metric distance from the local node.
    """

    NUM_BUCKETS = 160

    def __init__(self, node_id: NodeID, k: int = 20):
        self.node_id = node_id
        self.k = k
        self.buckets: List[KBucket] = [KBucket(k=k) for _ in range(self.NUM_BUCKETS)]

    def get_bucket_index(self, node_id: NodeID) -> int:
        """
        Calculate the bucket index for a target NodeID.
        Bucket index corresponds to floor(log2(distance)) -> (distance.bit_length() - 1).
        """
        dist = self.node_id.distance(node_id)
        if dist == 0:
            return 0
        bit_len = dist.bit_length()
        return min(bit_len - 1, self.NUM_BUCKETS - 1)

    def add_contact(self, node_info: NodeInfo) -> bool:
        """
        Add or refresh a contact in the appropriate K-Bucket.
        Refuses to add the local node itself.
        """
        if node_info.node_id == self.node_id:
            return False

        bucket_index = self.get_bucket_index(node_info.node_id)
        bucket = self.buckets[bucket_index]
        success = bucket.add(node_info)
        if success:
            logger.debug(f"Added/Updated contact {node_info.node_id.hex()[:8]} in bucket {bucket_index}")
        else:
            logger.debug(f"Bucket {bucket_index} is full. Contact placed in replacement cache.")
        return success

    def remove_contact(self, node_id: NodeID) -> Optional[NodeInfo]:
        """Remove a contact from its corresponding K-Bucket."""
        if node_id == self.node_id:
            return None
        bucket_index = self.get_bucket_index(node_id)
        return self.buckets[bucket_index].remove(node_id)

    def get_contact(self, node_id: NodeID) -> Optional[NodeInfo]:
        """Find a specific contact in the routing table if it exists."""
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
        Find up to `count` contacts in the routing table closest to `target_id`
        using XOR distance ordering.
        """
        limit = count if count is not None else self.k
        all_contacts: List[NodeInfo] = []

        for bucket in self.buckets:
            for contact in bucket.nodes:
                if exclude and contact.node_id == exclude:
                    continue
                if contact.node_id == target_id:
                    # Target itself can be included if known
                    all_contacts.append(contact)
                else:
                    all_contacts.append(contact)

        # Sort contacts by XOR distance to target_id
        all_contacts.sort(key=lambda contact: contact.node_id.distance(target_id))
        return all_contacts[:limit]

    def get_all_contacts(self) -> List[NodeInfo]:
        """Return a list of all active contacts in all buckets."""
        contacts: List[NodeInfo] = []
        for bucket in self.buckets:
            contacts.extend(bucket.nodes)
        return contacts

    def total_contacts(self) -> int:
        """Return the total number of contacts stored across all buckets."""
        return sum(len(bucket) for bucket in self.buckets)

    def __repr__(self) -> str:
        return f"<RoutingTable node_id={self.node_id.hex()[:8]}... total_contacts={self.total_contacts()}>"
