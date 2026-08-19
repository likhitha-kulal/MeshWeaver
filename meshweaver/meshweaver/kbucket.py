"""
MeshWeaver K-Bucket Implementation
Manages a fixed-capacity list (k=20) of peer node contacts ordered by time last seen.
"""

import time
from typing import List, Optional

from meshweaver.models import NodeID, NodeInfo


class KBucket:
    """
    Represents a single K-Bucket in the Kademlia routing table.
    Holds up to `k` contacts sorted from least-recently-seen (head)
    to most-recently-seen (tail).
    """

    def __init__(self, k: int = 20):
        self.k: int = k
        self.contacts: List[NodeInfo] = []
        self.replacement_cache: List[NodeInfo] = []
        self.last_updated: float = time.time()

    @property
    def nodes(self) -> List[NodeInfo]:
        """Return a copy of the list of active contacts."""
        return list(self.contacts)

    def is_full(self) -> bool:
        """Check if the bucket has reached capacity k."""
        return len(self.contacts) >= self.k

    def head(self) -> Optional[NodeInfo]:
        """Return the least-recently-seen contact (head of bucket)."""
        return self.contacts[0] if self.contacts else None

    def tail(self) -> Optional[NodeInfo]:
        """Return the most-recently-seen contact (tail of bucket)."""
        return self.contacts[-1] if self.contacts else None

    def get(self, node_id: NodeID) -> Optional[NodeInfo]:
        """Retrieve contact with matching NodeID if present in bucket."""
        for contact in self.contacts:
            if contact.node_id == node_id:
                return contact
        return None

    def add(self, node_info: NodeInfo) -> bool:
        """
        Add or update a node contact in the bucket.
        - If node is already present: move to tail (most recently seen) and update timestamp. Returns True.
        - If node is not present and bucket is not full: append to tail. Returns True.
        - If bucket is full: add to replacement cache if not already present. Returns False.
        """
        self.last_updated = time.time()
        node_info.last_seen = self.last_updated

        # 1. Existing node in primary contacts
        for idx, contact in enumerate(self.contacts):
            if contact.node_id == node_info.node_id:
                # Remove from current position and move to tail
                self.contacts.pop(idx)
                self.contacts.append(node_info)
                return True

        # 2. Bucket has space
        if len(self.contacts) < self.k:
            self.contacts.append(node_info)
            return True

        # 3. Bucket is full -> put into replacement cache
        for idx, candidate in enumerate(self.replacement_cache):
            if candidate.node_id == node_info.node_id:
                self.replacement_cache.pop(idx)
                break
        self.replacement_cache.append(node_info)
        # Cap replacement cache size to k
        if len(self.replacement_cache) > self.k:
            self.replacement_cache.pop(0)
        return False

    def remove(self, node_id: NodeID) -> Optional[NodeInfo]:
        """
        Remove contact from the bucket.
        If replacement cache has candidates, promote the first candidate into the bucket.
        """
        removed_contact: Optional[NodeInfo] = None
        for idx, contact in enumerate(self.contacts):
            if contact.node_id == node_id:
                removed_contact = self.contacts.pop(idx)
                break

        # Also remove from replacement cache if present
        self.replacement_cache = [c for c in self.replacement_cache if c.node_id != node_id]

        # Promote from replacement cache if available
        if removed_contact and self.replacement_cache and len(self.contacts) < self.k:
            promoted = self.replacement_cache.pop(0)
            self.contacts.append(promoted)

        return removed_contact

    def __len__(self) -> int:
        return len(self.contacts)

    def __contains__(self, node_id: NodeID) -> bool:
        return any(c.node_id == node_id for c in self.contacts)

    def __repr__(self) -> str:
        return f"<KBucket size={len(self.contacts)}/{self.k} replacements={len(self.replacement_cache)}>"
