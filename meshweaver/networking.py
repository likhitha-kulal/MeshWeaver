"""
MeshWeaver Networking Protocol
Asyncio DatagramProtocol for UDP node discovery, DHT routing, gossip heartbeats,
and TCP framing for task transport.
"""

import asyncio
import json
import logging
import struct
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from meshweaver.models import Message, MessageType, NodeID, NodeInfo, TaskResult
from meshweaver.routing_table import RoutingTable
from meshweaver.task_serializer import TaskSerializer

logger = logging.getLogger("meshweaver.networking")


class UDPNodeProtocol(asyncio.DatagramProtocol):
    """
    Non-blocking UDP protocol for discovery, routing table updates, RPC transactions,
    gossip health updates, and consensus leader election.
    """

    def __init__(
        self,
        node_id: NodeID,
        tcp_port: int = 0,
        routing_table: Optional[RoutingTable] = None,
        gossip_handler: Optional[Callable[[Dict[str, object]], None]] = None,
        consensus_vote_handler: Optional[Callable[[Message, Tuple[str, int]], Optional[Message]]] = None,
        consensus_heartbeat_handler: Optional[Callable[[Message, Tuple[str, int]], Optional[Message]]] = None,
        consensus_response_handler: Optional[Callable[[Message], None]] = None,
    ):
        self.node_id = node_id
        self.tcp_port = tcp_port
        self.routing_table = routing_table if routing_table is not None else RoutingTable(node_id)
        self.gossip_handler = gossip_handler
        self.consensus_vote_handler = consensus_vote_handler
        self.consensus_heartbeat_handler = consensus_heartbeat_handler
        self.consensus_response_handler = consensus_response_handler
        
        self.transport: Optional[asyncio.DatagramTransport] = None
        self._pending_requests: Dict[str, asyncio.Future[Message]] = {}
        self.local_udp_port: int = 0
        self.local_store: Dict[str, Tuple[Any, float]] = {}

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore
        sock = transport.get_extra_info("socket")
        if sock:
            self.local_udp_port = sock.getsockname()[1]
        logger.info(f"UDP service listening on port {self.local_udp_port} for node {self.node_id}")

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        """Process incoming UDP datagram packet."""
        try:
            json_str = data.decode("utf-8")
            msg = Message.from_json(json_str)
            logger.debug(f"[UDP RECV] {msg.type} from {addr[0]}:{addr[1]}")

            # Refresh sender contact in routing table
            try:
                sender_node_id = NodeID(msg.sender_id)
                sender_info = NodeInfo(
                    node_id=sender_node_id,
                    ip=addr[0],
                    udp_port=msg.sender_udp_port,
                    tcp_port=msg.sender_tcp_port if msg.sender_tcp_port > 0 else None,
                )
                self.routing_table.add_contact(sender_info)
            except Exception as e:
                logger.warning(f"Error parsing sender contact: {e}")

            if msg.type == MessageType.PING:
                self._handle_ping(msg, addr)
            elif msg.type == MessageType.FIND_NODE:
                self._handle_find_node(msg, addr)
            elif msg.type == MessageType.STORE:
                self._handle_store(msg, addr)
            elif msg.type == MessageType.FIND_VALUE:
                self._handle_find_value(msg, addr)
            elif msg.type == MessageType.GOSSIP:
                if self.gossip_handler is not None:
                    self.gossip_handler(msg.payload)
            elif msg.type == MessageType.ELECTION_VOTE_REQUEST:
                if self.consensus_vote_handler is not None:
                    resp = self.consensus_vote_handler(msg, addr)
                    if resp is not None:
                        self.send_datagram(resp, addr[0], addr[1])
            elif msg.type == MessageType.LEADER_HEARTBEAT:
                if self.consensus_heartbeat_handler is not None:
                    ack = self.consensus_heartbeat_handler(msg, addr)
                    if ack is not None:
                        self.send_datagram(ack, addr[0], addr[1])
            elif msg.type in (MessageType.ELECTION_VOTE_RESPONSE, MessageType.LEADER_HEARTBEAT_ACK):
                if self.consensus_response_handler is not None:
                    self.consensus_response_handler(msg)
                if msg.msg_id in self._pending_requests:
                    fut = self._pending_requests.pop(msg.msg_id)
                    if not fut.done():
                        fut.set_result(msg)
            elif msg.msg_id in self._pending_requests:
                fut = self._pending_requests.pop(msg.msg_id)
                if not fut.done():
                    fut.set_result(msg)

        except Exception as e:
            logger.error(f"[UDP ERROR] Exception parsing datagram from {addr}: {e}")

    def _handle_ping(self, ping_msg: Message, addr: Tuple[str, int]) -> None:
        pong_msg = Message(
            msg_id=ping_msg.msg_id,
            type=MessageType.PONG,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
            sender_tcp_port=self.tcp_port,
            payload={"status": "OK", "received_ping_id": ping_msg.msg_id},
        )
        self.send_datagram(pong_msg, addr[0], addr[1])

    def _handle_find_node(self, find_msg: Message, addr: Tuple[str, int]) -> None:
        target_hex = find_msg.payload.get("target_node_id")
        if not target_hex:
            return

        try:
            target_id = NodeID(target_hex)
            sender_node_id = NodeID(find_msg.sender_id)
        except Exception:
            return

        closest_contacts = self.routing_table.find_closest_nodes(
            target_id=target_id,
            count=self.routing_table.k,
            exclude=sender_node_id,
        )

        response_msg = Message(
            msg_id=find_msg.msg_id,
            type=MessageType.FIND_NODE_RESPONSE,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
            sender_tcp_port=self.tcp_port,
            payload={
                "nodes": [c.to_dict() for c in closest_contacts],
            },
        )
        self.send_datagram(response_msg, addr[0], addr[1])

    def _handle_store(self, store_msg: Message, addr: Tuple[str, int]) -> None:
        key = store_msg.payload.get("key")
        value = store_msg.payload.get("value")
        ttl = float(store_msg.payload.get("ttl", 3600.0))
        
        success = False
        if key is not None and value is not None:
            expires_at = time.time() + ttl
            self.local_store[key] = (value, expires_at)
            success = True

        resp_msg = Message(
            msg_id=store_msg.msg_id,
            type=MessageType.STORE_RESPONSE,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
            sender_tcp_port=self.tcp_port,
            payload={"success": success, "key": key},
        )
        self.send_datagram(resp_msg, addr[0], addr[1])

    def _handle_find_value(self, find_msg: Message, addr: Tuple[str, int]) -> None:
        key = find_msg.payload.get("key")
        sender_node_id = NodeID(find_msg.sender_id)

        if key in self.local_store:
            value, expires_at = self.local_store[key]
            if time.time() < expires_at:
                resp = Message(
                    msg_id=find_msg.msg_id,
                    type=MessageType.FIND_VALUE_RESPONSE,
                    sender_id=self.node_id.hex(),
                    sender_udp_port=self.local_udp_port,
                    sender_tcp_port=self.tcp_port,
                    payload={"found": True, "value": value},
                )
                self.send_datagram(resp, addr[0], addr[1])
                return

        target_id = NodeID.from_string_hash(key) if key else self.node_id
        closest_contacts = self.routing_table.find_closest_nodes(
            target_id=target_id,
            count=self.routing_table.k,
            exclude=sender_node_id,
        )

        resp = Message(
            msg_id=find_msg.msg_id,
            type=MessageType.FIND_VALUE_RESPONSE,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
            sender_tcp_port=self.tcp_port,
            payload={"found": False, "nodes": [c.to_dict() for c in closest_contacts]},
        )
        self.send_datagram(resp, addr[0], addr[1])

    def send_datagram(self, msg: Message, host: str, port: int) -> None:
        """Send a datagram to a remote host and UDP port."""
        if self.transport is None or self.transport.is_closing():
            return
        try:
            msg.sender_udp_port = self.local_udp_port
            msg.sender_tcp_port = self.tcp_port
            data = msg.to_json().encode("utf-8")
            self.transport.sendto(data, (host, port))
        except Exception as e:
            logger.error(f"Error sending datagram to {host}:{port}: {e}")

    async def send_rpc(self, msg: Message, host: str, port: int, timeout: float = 2.0) -> Message:
        """Send an RPC request and await matching response."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Message] = loop.create_future()
        self._pending_requests[msg.msg_id] = future

        self.send_datagram(msg, host, port)

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_requests.pop(msg.msg_id, None)
            raise TimeoutError(f"RPC {msg.type} to {host}:{port} timed out after {timeout}s")

    def send_gossip(self, host: str, port: int, payload: Dict[str, Any]) -> None:
        msg = Message(
            type=MessageType.GOSSIP,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
            sender_tcp_port=self.tcp_port,
            payload=payload,
        )
        self.send_datagram(msg, host, port)


class TCPTaskServer:
    """Length-prefixed binary streaming TCP server for task execution."""

    def __init__(self, node_id: NodeID, host: str = "127.0.0.1", port: int = 9001):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.server: Optional[asyncio.Server] = None
        self._running = False

    async def start(self) -> None:
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        sock = self.server.sockets[0]
        self.port = sock.getsockname()[1]
        self._running = True
        logger.info(f"TCP Task server listening on {self.host}:{self.port}")

    async def stop(self) -> None:
        self._running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info(f"TCP Task server stopped on {self.host}:{self.port}")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while self._running:
                header = await reader.readexactly(4)
                (payload_len,) = struct.unpack("!I", header)

                payload = await reader.readexactly(payload_len)
                msg = Message.from_json(payload.decode("utf-8"))

                if msg.type == MessageType.TASK_EXECUTE:
                    task_id = msg.payload.get("task_id", "unknown")
                    envelope_data = msg.payload.get("envelope", {})
                    task_result = await TaskSerializer.execute_from_dict(envelope_data, task_id=task_id)

                    result_msg = Message(
                        msg_id=msg.msg_id,
                        type=MessageType.TASK_RESULT,
                        sender_id=self.node_id.hex(),
                        sender_udp_port=0,
                        sender_tcp_port=self.port,
                        payload=task_result.to_dict(),
                    )
                    resp_data = result_msg.to_json().encode("utf-8")
                    resp_header = struct.pack("!I", len(resp_data))

                    writer.write(resp_header + resp_data)
                    await writer.drain()

        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception as e:
            logger.error(f"TCP handler error: {e}", exc_info=True)
        finally:
            writer.close()
            await writer.wait_closed()


class TCPTaskClient:
    """Client for dispatching framed binary tasks over TCP."""

    @staticmethod
    async def execute_remote_task(
        host: str,
        port: int,
        task_envelope: Dict[str, Any],
        task_id: str,
        sender_id: str,
        timeout: float = 10.0,
    ) -> TaskResult:
        reader, writer = await asyncio.open_connection(host, port)
        try:
            msg = Message(
                type=MessageType.TASK_EXECUTE,
                sender_id=sender_id,
                sender_udp_port=0,
                sender_tcp_port=0,
                payload={"task_id": task_id, "envelope": task_envelope},
            )
            data = msg.to_json().encode("utf-8")
            header = struct.pack("!I", len(data))

            writer.write(header + data)
            await writer.drain()

            resp_header = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
            (resp_len,) = struct.unpack("!I", resp_header)

            resp_data = await asyncio.wait_for(reader.readexactly(resp_len), timeout=timeout)
            result_msg = Message.from_json(resp_data.decode("utf-8"))

            return TaskResult.from_dict(result_msg.payload)

        finally:
            writer.close()
            await writer.wait_closed()
