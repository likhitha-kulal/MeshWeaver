"""
MeshWeaver K-Bucket
Maintains a fixed-capacity contact list (k=20) ordered by time last seen (LRU eviction).
"""

import time
from typing import List, Optional

from meshweaver.models import NodeID, NodeInfo


class KBucket:
    """
    Single K-Bucket in the routing table.
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
        """Return active contacts list."""
        return list(self.contacts)

    def is_full(self) -> bool:
        """Check if bucket is at capacity."""
        return len(self.contacts) >= self.k

    def head(self) -> Optional[NodeInfo]:
        """Return least-recently-seen contact."""
        return self.contacts[0] if self.contacts else None

    def tail(self) -> Optional[NodeInfo]:
        """Return most-recently-seen contact."""
        return self.contacts[-1] if self.contacts else None

    def get(self, node_id: NodeID) -> Optional[NodeInfo]:
        """Retrieve contact with matching NodeID if present."""
        for contact in self.contacts:
            if contact.node_id == node_id:
                return contact
        return None

    def add(self, node_info: NodeInfo) -> bool:
        """
        Add or refresh a contact in the bucket.
        - Existing node: moved to tail and timestamp refreshed. Returns True.
        - Space available: appended to tail. Returns True.
        - Bucket full: stored in replacement cache. Returns False.
        """
        self.last_updated = time.time()
        node_info.last_seen = self.last_updated

        # 1. Refresh existing contact
        for idx, contact in enumerate(self.contacts):
            if contact.node_id == node_info.node_id:
                self.contacts.pop(idx)
                self.contacts.append(node_info)
                return True

        # 2. Append if space available
        if len(self.contacts) < self.k:
            self.contacts.append(node_info)
            return True

        # 3. Bucket full -> add to replacement cache
        for idx, candidate in enumerate(self.replacement_cache):
            if candidate.node_id == node_info.node_id:
                self.replacement_cache.pop(idx)
                break
        self.replacement_cache.append(node_info)
        if len(self.replacement_cache) > self.k:
            self.replacement_cache.pop(0)
        return False

    def remove(self, node_id: NodeID) -> Optional[NodeInfo]:
        """
        Remove contact from the bucket.
        Promotes oldest replacement candidate if present.
        """
        removed_contact: Optional[NodeInfo] = None
        for idx, contact in enumerate(self.contacts):
            if contact.node_id == node_id:
                removed_contact = self.contacts.pop(idx)
                break

        self.replacement_cache = [c for c in self.replacement_cache if c.node_id != node_id]

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
