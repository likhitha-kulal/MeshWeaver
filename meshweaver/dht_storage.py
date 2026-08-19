"""High-level distributed key/value storage over the MeshWeaver DHT."""

import asyncio
from typing import Any, Dict, Optional, Set

from meshweaver.models import NodeID
from meshweaver.networking import UDPNodeProtocol
from meshweaver.node_lookup import NodeLookup


class DHTStorage:
    """Coordinate iterative peer lookup with STORE and FIND_VALUE RPCs."""

    def __init__(self, protocol: UDPNodeProtocol, lookup: Optional[NodeLookup] = None):
        self.protocol = protocol
        self.lookup = lookup or NodeLookup(protocol)

    async def store(self, key: str, value: Any, ttl: float, timeout: float = 5.0) -> int:
        """Store a value on every peer returned by the iterative lookup."""
        peers = await self.lookup.lookup(NodeID.from_string_hash(key), timeout=timeout)
        results = await asyncio.gather(
            *(
                self.protocol.send_store(
                    target_ip=peer.ip,
                    target_port=peer.udp_port,
                    key=key,
                    value=value,
                    ttl=ttl,
                    timeout=timeout,
                )
                for peer in peers
            ),
            return_exceptions=True,
        )
        return sum(result is True for result in results)

    async def find_value(self, key: str, timeout: float = 5.0) -> Any:
        """Return the first value found among peers, or None when absent."""
        peers = await self.lookup.lookup(NodeID.from_string_hash(key), timeout=timeout)
        pending: Set[asyncio.Task[Dict[str, Any]]] = {
            asyncio.create_task(
                self.protocol.send_find_value(
                    target_ip=peer.ip,
                    target_port=peer.udp_port,
                    key=key,
                    timeout=timeout,
                )
            )
            for peer in peers
        }

        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    try:
                        response = task.result()
                    except Exception:
                        continue
                    if response.get("found") is True:
                        for remaining in pending:
                            remaining.cancel()
                        await asyncio.gather(*pending, return_exceptions=True)
                        return response.get("value")
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

        return None