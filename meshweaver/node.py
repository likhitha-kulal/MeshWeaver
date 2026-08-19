"""
MeshNode Entrypoint (Networking & Infra Track - Person A)
Main class and CLI entrypoint for launching a MeshWeaver Peer Node (UDP Server & DHT Routing).
"""

import argparse
import asyncio
import logging
import os
import sys
from typing import List, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.models import Message, NodeID, NodeInfo
from meshweaver.networking import UDPNodeProtocol
from meshweaver.routing_table import RoutingTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("meshweaver.node")


class MeshNode:
    """
    MeshWeaver peer-to-peer node managing UDP discovery protocol (DatagramProtocol)
    and Kademlia DHT routing table.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        udp_port: int = 9000,
        node_id: Optional[NodeID] = None,
        k: int = 20,
    ):
        self.host = host
        self.requested_udp_port = udp_port
        self.node_id = node_id or NodeID()
        self.routing_table = RoutingTable(self.node_id, k=k)

        self.udp_transport: Optional[asyncio.DatagramTransport] = None
        self.udp_protocol: Optional[UDPNodeProtocol] = None

        self.bound_udp_port: int = 0

    @property
    def info(self) -> NodeInfo:
        return NodeInfo(
            node_id=self.node_id,
            ip=self.host,
            udp_port=self.bound_udp_port,
        )

    async def start(self) -> None:
        """Start UDP server for the node."""
        loop = asyncio.get_running_loop()

        udp_factory = lambda: UDPNodeProtocol(
            node_id=self.node_id,
            routing_table=self.routing_table,
        )
        transport, protocol = await loop.create_datagram_endpoint(
            udp_factory,
            local_addr=(self.host, self.requested_udp_port),
        )
        self.udp_transport = transport  # type: ignore
        self.udp_protocol = protocol  # type: ignore
        self.bound_udp_port = self.udp_protocol.local_udp_port

        logger.info(
            f"=== MeshWeaver Node Started (UDP & DHT Routing Table) ===\n"
            f"  Node ID  : {self.node_id.hex()}\n"
            f"  Host     : {self.host}\n"
            f"  UDP Port : {self.bound_udp_port}\n"
            f"==========================================================="
        )

    async def stop(self) -> None:
        """Stop UDP server and release port."""
        if self.udp_transport:
            self.udp_transport.close()
        logger.info("MeshNode stopped.")

    async def ping(self, target_host: str, target_udp_port: int, timeout: float = 5.0) -> Message:
        """Ping a remote node over UDP datagram protocol."""
        if not self.udp_protocol:
            raise RuntimeError("Node is not running")
        return await self.udp_protocol.send_ping(target_host, target_udp_port, timeout=timeout)

    async def find_node(
        self,
        target_host: str,
        target_udp_port: int,
        target_id: NodeID,
        timeout: float = 5.0,
    ) -> List[NodeInfo]:
        """
        Send FIND_NODE RPC to a peer querying for closest contacts to target_id.
        """
        if not self.udp_protocol:
            raise RuntimeError("Node is not running")
        return await self.udp_protocol.send_find_node(
            target_ip=target_host,
            target_port=target_udp_port,
            target_id=target_id,
            timeout=timeout,
        )

    async def bootstrap(
        self,
        bootstrap_nodes: List[Tuple[str, int]],
        timeout: float = 5.0,
    ) -> int:
        """
        Join network by contacting initial bootstrap nodes.
        1. Pings bootstrap nodes to populate contact info.
        2. Sends FIND_NODE for self.node_id to discover closest peers in network.
        Returns the number of contacts successfully added to routing table.
        """
        if not self.udp_protocol:
            raise RuntimeError("Node is not running")

        initial_count = self.routing_table.total_contacts()
        logger.info(f"Starting bootstrap sequence with {len(bootstrap_nodes)} bootstrap nodes...")

        for ip, port in bootstrap_nodes:
            try:
                # 1. Ping bootstrap node
                pong = await self.ping(ip, port, timeout=timeout)
                logger.info(f"Bootstrap peer {ip}:{port} reachable (Node ID: {pong.sender_id[:8]}...)")

                # 2. Query for nodes closest to self
                discovered = await self.find_node(ip, port, self.node_id, timeout=timeout)
                logger.info(f"Bootstrap query returned {len(discovered)} nodes from {ip}:{port}")

                # 3. Ping each newly discovered node to verify and populate routing table
                for contact in discovered:
                    if contact.node_id != self.node_id:
                        try:
                            await self.ping(contact.ip, contact.udp_port, timeout=2.0)
                        except Exception:
                            logger.debug(f"Could not ping discovered peer {contact.ip}:{contact.udp_port}")

            except Exception as e:
                logger.warning(f"Failed to bootstrap with node at {ip}:{port}: {e}")

        final_count = self.routing_table.total_contacts()
        logger.info(f"Bootstrap finished. Routing table grew from {initial_count} to {final_count} contacts.")
        return final_count - initial_count


async def cli_main() -> None:
    parser = argparse.ArgumentParser(description="MeshWeaver P2P Node (Networking & DHT Track - Person A)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Binding host address")
    parser.add_argument("--port", type=int, default=9000, help="UDP port")
    parser.add_argument("--ping-host", type=str, default=None, help="Target host to PING on startup")
    parser.add_argument("--ping-port", type=int, default=None, help="Target UDP port to PING on startup")
    parser.add_argument("--bootstrap-host", type=str, default=None, help="Bootstrap node host")
    parser.add_argument("--bootstrap-port", type=int, default=None, help="Bootstrap node UDP port")
    parser.add_argument("--find-node", type=str, default=None, help="Target NodeID hex to lookup via FIND_NODE")

    args = parser.parse_args()

    node = MeshNode(
        host=args.host,
        udp_port=args.port,
    )

    await node.start()

    try:
        if args.ping_host and args.ping_port:
            logger.info(f"--- Sending PING to {args.ping_host}:{args.ping_port} ---")
            pong = await node.ping(args.ping_host, args.ping_port)
            logger.info(f"PING Success! Received PONG from node {pong.sender_id[:8]}... (status={pong.payload.get('status')})")

        if args.bootstrap_host and args.bootstrap_port:
            logger.info(f"--- Bootstrapping via {args.bootstrap_host}:{args.bootstrap_port} ---")
            await node.bootstrap([(args.bootstrap_host, args.bootstrap_port)])

        if args.find_node and args.ping_host and args.ping_port:
            target_id = NodeID(args.find_node)
            logger.info(f"--- Querying FIND_NODE for {target_id.hex()} via {args.ping_host}:{args.ping_port} ---")
            nodes = await node.find_node(args.ping_host, args.ping_port, target_id)
            logger.info(f"FIND_NODE returned {len(nodes)} closest nodes:")
            for n in nodes:
                logger.info(f"  -> {n.node_id.hex()} @ {n.ip}:{n.udp_port}")

        if not (args.ping_host or args.bootstrap_host):
            logger.info("Node running in UDP listener mode. Press Ctrl+C to stop.")
            await asyncio.Event().wait()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(cli_main())
