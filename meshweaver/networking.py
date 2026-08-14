"""
MeshWeaver Networking Protocol Implementation (Networking & Infra Track)
Provides asyncio.DatagramProtocol for UDP node discovery and PING-PONG RPC messaging.
"""

import asyncio
import logging
from typing import Dict, Optional, Tuple

from meshweaver.models import Message, MessageType, NodeID

logger = logging.getLogger("meshweaver.networking")


class UDPNodeProtocol(asyncio.DatagramProtocol):
    """
    Asyncio DatagramProtocol implementation for MeshWeaver UDP communication.
    Handles ping/pong node discovery and non-blocking RPC messaging.
    """

    def __init__(self, node_id: NodeID):
        self.node_id = node_id
        self.transport: Optional[asyncio.DatagramTransport] = None
        self._pending_requests: Dict[str, asyncio.Future[Message]] = {}
        self.local_udp_port: int = 0

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore
        sock = transport.get_extra_info("socket")
        if sock:
            self.local_udp_port = sock.getsockname()[1]
        logger.info(f"UDP server bound on port {self.local_udp_port} for node {self.node_id}")

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        """Called by asyncio event loop when a UDP datagram is received."""
        try:
            json_str = data.decode("utf-8")
            msg = Message.from_json(json_str)
            logger.info(f"[UDP RECV] {msg.type} from {addr[0]}:{addr[1]} (Sender ID: {msg.sender_id[:8]}..., msg_id={msg.msg_id[:8]})")

            if msg.type == MessageType.PING:
                self._handle_ping(msg, addr)
            elif msg.msg_id in self._pending_requests:
                fut = self._pending_requests.pop(msg.msg_id)
                if not fut.done():
                    fut.set_result(msg)
            else:
                logger.debug(f"[UDP UNHANDLED] Message {msg.msg_id[:8]} type {msg.type} ignored")

        except Exception as e:
            logger.error(f"[UDP ERROR] Exception handling incoming datagram from {addr}: {e}", exc_info=True)

    def _handle_ping(self, ping_msg: Message, addr: Tuple[str, int]) -> None:
        """Respond to PING with PONG datagram."""
        pong_msg = Message(
            msg_id=ping_msg.msg_id,  # Echo back same msg_id for transaction matching
            type=MessageType.PONG,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
            payload={"status": "OK", "received_ping_id": ping_msg.msg_id},
        )
        self.send_datagram(pong_msg, addr[0], addr[1])
        logger.info(f"[UDP SENT] PONG to {addr[0]}:{addr[1]} (msg_id={pong_msg.msg_id[:8]})")

    def send_datagram(self, msg: Message, target_ip: str, target_port: int) -> None:
        """Send raw JSON encoded datagram to target endpoint."""
        if not self.transport:
            raise RuntimeError("UDP transport is not connected")
        payload_bytes = msg.to_json().encode("utf-8")
        self.transport.sendto(payload_bytes, (target_ip, target_port))

    async def send_ping(self, target_ip: str, target_port: int, timeout: float = 5.0) -> Message:
        """
        Send PING to target node and wait for PONG response.
        """
        ping_msg = Message(
            type=MessageType.PING,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
        )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Message] = loop.create_future()
        self._pending_requests[ping_msg.msg_id] = future

        logger.info(f"[UDP SENT] PING to {target_ip}:{target_port} (msg_id={ping_msg.msg_id[:8]})")
        self.send_datagram(ping_msg, target_ip, target_port)

        try:
            response = await asyncio.wait_for(future, timeout=timeout)
            return response
        except asyncio.TimeoutError:
            self._pending_requests.pop(ping_msg.msg_id, None)
            logger.warning(f"[UDP TIMEOUT] PING to {target_ip}:{target_port} timed out after {timeout}s")
            raise TimeoutError(f"PING to {target_ip}:{target_port} timed out after {timeout}s")

    def error_received(self, exc: Exception) -> None:
        logger.error(f"[UDP ERROR] Protocol error: {exc}")

    def connection_lost(self, exc: Optional[Exception]) -> None:
        logger.info(f"UDP connection closed: {exc}")
