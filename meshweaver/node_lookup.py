"""Iterative Kademlia node lookups."""

import asyncio
from typing import List, Optional, Set

from meshweaver.models import NodeID, NodeInfo
from meshweaver.networking import UDPNodeProtocol
from meshweaver.routing_table import RoutingTable


class NodeLookup:
    """Perform an iterative nearest-node lookup over UDP FIND_NODE RPCs."""

    def __init__(
        self,
        protocol: UDPNodeProtocol,
        routing_table: Optional[RoutingTable] = None,
        alpha: int = 3,
    ):
        if alpha < 1:
            raise ValueError("alpha must be at least 1")

        self.protocol = protocol
        self.routing_table = routing_table or protocol.routing_table
        self.alpha = alpha

    async def lookup(self, target_id: NodeID, timeout: float = 5.0) -> List[NodeInfo]:
        """Return the closest known contacts after iterative discovery."""
        queried: Set[NodeID] = set()

        while True:
            closest_before = self.routing_table.find_closest_nodes(
                target_id=target_id,
                count=self.routing_table.k,
            )
            candidates = [
                contact
                for contact in closest_before
                if contact.node_id not in queried
            ][: self.alpha]
            if not candidates:
                break

            queried.update(contact.node_id for contact in candidates)
            responses = await asyncio.gather(
                *(
                    self.protocol.send_find_node(
                        target_ip=contact.ip,
                        target_port=contact.udp_port,
                        target_id=target_id,
                        timeout=timeout,
                    )
                    for contact in candidates
                ),
                return_exceptions=True,
            )

            for response in responses:
                if isinstance(response, BaseException):
                    continue
                for contact in response:
                    self.routing_table.add_contact(contact)

            closest_after = self.routing_table.find_closest_nodes(
                target_id=target_id,
                count=self.routing_table.k,
            )
            if self._closest_distance(closest_after, target_id) >= self._closest_distance(
                closest_before, target_id
            ):
                break

        return self.routing_table.find_closest_nodes(
            target_id=target_id,
            count=self.routing_table.k,
        )

    @staticmethod
    def _closest_distance(contacts: List[NodeInfo], target_id: NodeID) -> int:
        if not contacts:
            return NodeID.ID_BIT_LENGTH + 1
        return contacts[0].node_id.distance(target_id)