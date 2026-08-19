"""
MeshNode Entrypoint
Main class and CLI entrypoint for launching a MeshWeaver Peer Node.
"""

import argparse
import asyncio
import logging
import os
import sys
from typing import Any, Callable, List, Optional, Tuple

# Ensure package import path is available when executed as script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.gossip import GossipManager
from meshweaver.models import Message, NodeID, NodeInfo
from meshweaver.networking import TCPTaskClient, TCPTaskServer, UDPNodeProtocol
from meshweaver.routing_table import RoutingTable
from meshweaver.task_serializer import RemoteExecutionError, TaskSerializer

# Configure root logger format for CLI visibility
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("meshweaver.node")


class MeshNode:
    """
    MeshWeaver peer-to-peer node combining UDP discovery & DHT routing table (Person A)
    and TCP task execution server (Person B).
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        udp_port: int = 9000,
        tcp_port: Optional[int] = None,
        node_id: Optional[NodeID] = None,
        k: int = 20,
    ):
        self.host = host
        self.requested_udp_port = udp_port
        self.requested_tcp_port = tcp_port if tcp_port is not None else (udp_port + 1)
        self.node_id = node_id or NodeID()
        self.routing_table = RoutingTable(self.node_id, k=k)

        self.udp_transport: Optional[asyncio.DatagramTransport] = None
        self.udp_protocol: Optional[UDPNodeProtocol] = None
        self.tcp_server: Optional[TCPTaskServer] = None
        self.gossip_manager = GossipManager(
            node_id=self.node_id.hex(),
            host=self.host,
            udp_port=udp_port,
            heartbeat_interval=5.0,
            dead_node_timeout=15.0,
        )

        self.bound_udp_port: int = 0
        self.bound_tcp_port: int = 0

    @property
    def info(self) -> NodeInfo:
        return NodeInfo(
            node_id=self.node_id,
            ip=self.host,
            udp_port=self.bound_udp_port,
            tcp_port=self.bound_tcp_port,
        )

    async def start(self) -> None:
        """Start UDP and TCP servers for the node."""
        loop = asyncio.get_running_loop()

        # 1. Start TCP Task Server
        self.tcp_server = TCPTaskServer(
            node_id=self.node_id,
            host=self.host,
            port=self.requested_tcp_port,
        )
        await self.tcp_server.start()
        self.bound_tcp_port = self.tcp_server.port

        # 2. Start UDP Protocol Endpoint
        self.gossip_manager.set_send_callback(
            lambda host, port, payload: self.udp_protocol.send_gossip(host, port, payload)
            if self.udp_protocol is not None else None
        )
        self.gossip_manager.set_receive_callback(self.gossip_manager.receive_heartbeat)

        udp_factory = lambda: UDPNodeProtocol(
            node_id=self.node_id,
            tcp_port=self.bound_tcp_port,
            routing_table=self.routing_table,
            gossip_handler=self.gossip_manager.receive_heartbeat,
        )
        transport, protocol = await loop.create_datagram_endpoint(
            udp_factory,
            local_addr=(self.host, self.requested_udp_port),
        )
        self.udp_transport = transport  # type: ignore
        self.udp_protocol = protocol  # type: ignore
        self.bound_udp_port = self.udp_protocol.local_udp_port
        self.gossip_manager.bind_network(self.udp_protocol)
        self.gossip_manager.set_send_callback(
            lambda host, port, payload: self.udp_protocol.send_gossip(host, port, payload)
            if self.udp_protocol is not None else None
        )
        await self.gossip_manager.start()

        logger.info(
            f"=== MeshWeaver Node Started ===\n"
            f"  Node ID  : {self.node_id.hex()}\n"
            f"  Host     : {self.host}\n"
            f"  UDP Port : {self.bound_udp_port}\n"
            f"  TCP Port : {self.bound_tcp_port}\n"
            f"================================"
        )

    async def stop(self) -> None:
        """Stop node services and release ports."""
        await self.gossip_manager.stop()
        if self.tcp_server:
            await self.tcp_server.stop()
        if self.udp_transport:
            self.udp_transport.close()
        logger.info("MeshNode stopped.")

    def register_neighbor(self, node_id: str, host: str, udp_port: int, tcp_port: Optional[int] = None) -> None:
        """Register a known neighbor for gossip heartbeats."""
        self.gossip_manager.register_neighbor(node_id, host, udp_port, tcp_port)

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
        """Send FIND_NODE RPC to a peer querying for closest contacts to target_id."""
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

    async def submit_task(self, target_host: str, target_tcp_port: int, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Serialize a task and submit it to a remote node's TCPTaskServer for execution."""
        payload_bytes = TaskSerializer.serialize(func, *args, **kwargs)
        task_result = await TCPTaskClient.send_task(target_host, target_tcp_port, payload_bytes)
        return TaskSerializer.unpack_result(task_result)


# --- Demo Functions for CLI testing ---
def sample_add(a: int, b: int) -> int:
    """Sample addition task."""
    return a + b


def sample_fibonacci(n: int) -> int:
    """Sample fibonacci calculation task."""
    if n <= 0:
        return 0
    elif n == 1:
        return 1
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b


def sample_failing_task() -> float:
    """Sample task that intentionally raises ZeroDivisionError."""
    return 1 / 0


async def cli_main() -> None:
    parser = argparse.ArgumentParser(description="MeshWeaver P2P Node")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Binding host address")
    parser.add_argument("--port", type=int, default=9000, help="Base UDP port (TCP port defaults to port + 1)")
    parser.add_argument("--tcp-port", type=int, default=None, help="Explicit TCP port for task server")
    parser.add_argument("--ping-host", type=str, default=None, help="Target host to PING on startup")
    parser.add_argument("--ping-port", type=int, default=None, help="Target UDP port to PING on startup")
    parser.add_argument("--bootstrap-host", type=str, default=None, help="Bootstrap node host")
    parser.add_argument("--bootstrap-port", type=int, default=None, help="Bootstrap node UDP port")
    parser.add_argument("--find-node", type=str, default=None, help="Target NodeID hex to lookup via FIND_NODE")
    parser.add_argument("--task-target-port", type=int, default=None, help="Target TCP port to send demo tasks to")
    parser.add_argument("--demo-task", action="store_true", help="Submit demo tasks to target node")

    args = parser.parse_args()

    node = MeshNode(
        host=args.host,
        udp_port=args.port,
        tcp_port=args.tcp_port,
    )

    await node.start()

    try:
        # Perform PING demo if requested
        if args.ping_host and args.ping_port:
            logger.info(f"--- Sending PING to {args.ping_host}:{args.ping_port} ---")
            pong = await node.ping(args.ping_host, args.ping_port)
            logger.info(f"PING Success! Received PONG response from node {pong.sender_id[:8]}... (status={pong.payload.get('status')})")

        # Perform Bootstrap if requested
        if args.bootstrap_host and args.bootstrap_port:
            logger.info(f"--- Bootstrapping via {args.bootstrap_host}:{args.bootstrap_port} ---")
            await node.bootstrap([(args.bootstrap_host, args.bootstrap_port)])

        # Perform FIND_NODE if requested
        if args.find_node and args.ping_host and args.ping_port:
            target_id = NodeID(args.find_node)
            logger.info(f"--- Querying FIND_NODE for {target_id.hex()} via {args.ping_host}:{args.ping_port} ---")
            nodes = await node.find_node(args.ping_host, args.ping_port, target_id)
            logger.info(f"FIND_NODE returned {len(nodes)} closest nodes:")
            for n in nodes:
                logger.info(f"  -> {n.node_id.hex()} @ {n.ip}:{n.udp_port}")

        # Perform Task Execution demo if requested
        if args.demo_task and args.task_target_port:
            logger.info(f"--- Sending Remote Task Execution Demos to {args.host}:{args.task_target_port} ---")
            
            # 1. Fibonacci task
            n = 20
            logger.info(f"Submitting remote task: sample_fibonacci({n})")
            fib_res = await node.submit_task(args.host, args.task_target_port, sample_fibonacci, n)
            logger.info(f"Remote Task Result: sample_fibonacci({n}) = {fib_res}")

            # 2. Add task
            logger.info("Submitting remote task: sample_add(42, 58)")
            add_res = await node.submit_task(args.host, args.task_target_port, sample_add, 42, 58)
            logger.info(f"Remote Task Result: sample_add(42, 58) = {add_res}")

            # 3. Error Handling test
            logger.info("Submitting failing remote task: sample_failing_task()")
            try:
                await node.submit_task(args.host, args.task_target_port, sample_failing_task)
            except RemoteExecutionError as err:
                logger.info(f"Caught RemoteExecutionError gracefully as expected:\n  Error Type: {err.error_type}\n  Message   : {err.error_message}")

        if not (args.ping_host or args.bootstrap_host or args.demo_task):
            logger.info("Node is running in server mode. Press Ctrl+C to stop.")
            await asyncio.Event().wait()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(cli_main())
