"""
MeshNode Entrypoint
Main class and CLI entrypoint for launching a MeshWeaver Peer Node.
"""

import argparse
import asyncio
import logging
import os
import sys
from typing import Any, Callable, Optional

# Ensure package import path is available when executed as script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.models import Message, NodeID, NodeInfo
from meshweaver.networking import TCPTaskClient, TCPTaskServer, UDPNodeProtocol
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
    MeshWeaver peer-to-peer node combining UDP discovery protocol
    and TCP task execution server.
    """

    def __init__(self, host: str = "127.0.0.1", udp_port: int = 9000, tcp_port: Optional[int] = None, node_id: Optional[NodeID] = None):
        self.host = host
        self.requested_udp_port = udp_port
        self.requested_tcp_port = tcp_port if tcp_port is not None else (udp_port + 1)
        self.node_id = node_id or NodeID()

        self.udp_transport: Optional[asyncio.DatagramTransport] = None
        self.udp_protocol: Optional[UDPNodeProtocol] = None
        self.tcp_server: Optional[TCPTaskServer] = None

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
        udp_factory = lambda: UDPNodeProtocol(node_id=self.node_id, tcp_port=self.bound_tcp_port)
        transport, protocol = await loop.create_datagram_endpoint(
            udp_factory,
            local_addr=(self.host, self.requested_udp_port),
        )
        self.udp_transport = transport  # type: ignore
        self.udp_protocol = protocol  # type: ignore
        self.bound_udp_port = self.udp_protocol.local_udp_port

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
        if self.tcp_server:
            await self.tcp_server.stop()
        if self.udp_transport:
            self.udp_transport.close()
        logger.info("MeshNode stopped.")

    async def ping(self, target_host: str, target_udp_port: int, timeout: float = 5.0) -> Message:
        """Ping a remote node over UDP datagram protocol."""
        if not self.udp_protocol:
            raise RuntimeError("Node is not running")
        return await self.udp_protocol.send_ping(target_host, target_udp_port, timeout=timeout)

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
    """Sample task that intentional raises ZeroDivisionError."""
    return 1 / 0


async def cli_main() -> None:
    parser = argparse.ArgumentParser(description="MeshWeaver P2P Node")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Binding host address")
    parser.add_argument("--port", type=int, default=9000, help="Base UDP port (TCP port defaults to port + 1)")
    parser.add_argument("--tcp-port", type=int, default=None, help="Explicit TCP port for task server")
    parser.add_argument("--ping-host", type=str, default=None, help="Target host to PING on startup")
    parser.add_argument("--ping-port", type=int, default=None, help="Target UDP port to PING on startup")
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

        if not (args.ping_host or args.demo_task):
            logger.info("Node is running in server mode. Press Ctrl+C to stop.")
            # Keep running until cancelled
            await asyncio.Event().wait()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(cli_main())
