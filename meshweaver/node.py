"""
MeshNode Entrypoint (Networking & Infra Track - Person A)
Main class and CLI entrypoint for launching a MeshWeaver Peer Node (UDP Server).
"""

import argparse
import asyncio
import logging
import os
import sys
from typing import Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.models import Message, NodeID, NodeInfo
from meshweaver.networking import UDPNodeProtocol

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("meshweaver.node")


class MeshNode:
    """
    MeshWeaver peer-to-peer node managing UDP discovery protocol (DatagramProtocol).
    """

    def __init__(self, host: str = "127.0.0.1", udp_port: int = 9000, node_id: Optional[NodeID] = None):
        self.host = host
        self.requested_udp_port = udp_port
        self.node_id = node_id or NodeID()

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

        udp_factory = lambda: UDPNodeProtocol(node_id=self.node_id)
        transport, protocol = await loop.create_datagram_endpoint(
            udp_factory,
            local_addr=(self.host, self.requested_udp_port),
        )
        self.udp_transport = transport  # type: ignore
        self.udp_protocol = protocol  # type: ignore
        self.bound_udp_port = self.udp_protocol.local_udp_port

        logger.info(
            f"=== MeshWeaver Node Started (UDP Server) ===\n"
            f"  Node ID  : {self.node_id.hex()}\n"
            f"  Host     : {self.host}\n"
            f"  UDP Port : {self.bound_udp_port}\n"
            f"============================================"
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


async def cli_main() -> None:
    parser = argparse.ArgumentParser(description="MeshWeaver P2P Node (Networking Track)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Binding host address")
    parser.add_argument("--port", type=int, default=9000, help="UDP port")
    parser.add_argument("--ping-host", type=str, default=None, help="Target host to PING on startup")
    parser.add_argument("--ping-port", type=int, default=None, help="Target UDP port to PING on startup")

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
            logger.info(f"PING Success! Received PONG response from node {pong.sender_id[:8]}... (status={pong.payload.get('status')})")
        else:
            logger.info("Node running in UDP listener mode. Press Ctrl+C to stop.")
            await asyncio.Event().wait()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(cli_main())
