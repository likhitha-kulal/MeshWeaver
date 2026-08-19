"""
MeshWeaver Networking Protocol
Asyncio DatagramProtocol for UDP node discovery, DHT routing, gossip heartbeats,
and TCP framing for task transport.
"""

import asyncio
import json
import logging
import struct
from typing import Callable, Dict, List, Optional, Tuple

from meshweaver.models import Message, MessageType, NodeID, NodeInfo, TaskResult
from meshweaver.routing_table import RoutingTable
from meshweaver.task_serializer import TaskSerializer

logger = logging.getLogger("meshweaver.networking")


class UDPNodeProtocol(asyncio.DatagramProtocol):
    """
    Non-blocking UDP protocol for discovery, routing table updates, and RPC transactions.
    """

    def __init__(
        self,
        node_id: NodeID,
        tcp_port: int = 0,
        routing_table: Optional[RoutingTable] = None,
        gossip_handler: Optional[Callable[[Dict[str, object]], None]] = None,
    ):
        self.node_id = node_id
        self.tcp_port = tcp_port
        self.routing_table = routing_table if routing_table is not None else RoutingTable(node_id)
        self.gossip_handler = gossip_handler
        self.transport: Optional[asyncio.DatagramTransport] = None
        self._pending_requests: Dict[str, asyncio.Future[Message]] = {}
        self.local_udp_port: int = 0

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
            elif msg.type == MessageType.GOSSIP:
                if self.gossip_handler is not None:
                    self.gossip_handler(msg.payload)
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
                "target_node_id": target_hex,
                "nodes": [c.to_dict() for c in closest_contacts],
            },
        )
        self.send_datagram(response_msg, addr[0], addr[1])

    def send_datagram(self, msg: Message, target_ip: str, target_port: int) -> None:
        """Send serialized JSON datagram to remote peer."""
        if not self.transport:
            raise RuntimeError("UDP transport is inactive")
        payload_bytes = msg.to_json().encode("utf-8")
        self.transport.sendto(payload_bytes, (target_ip, target_port))

    def send_gossip(self, target_ip: str, target_port: int, payload: Dict[str, object]) -> None:
        """Send gossip packet to target peer."""
        gossip_msg = Message(
            type=MessageType.GOSSIP,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
            sender_tcp_port=self.tcp_port,
            payload=payload,
        )
        self.send_datagram(gossip_msg, target_ip, target_port)

    async def send_ping(self, target_ip: str, target_port: int, timeout: float = 5.0) -> Message:
        """Send PING RPC and await PONG response."""
        ping_msg = Message(
            type=MessageType.PING,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
            sender_tcp_port=self.tcp_port,
        )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Message] = loop.create_future()
        self._pending_requests[ping_msg.msg_id] = future
        self.send_datagram(ping_msg, target_ip, target_port)

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_requests.pop(ping_msg.msg_id, None)
            raise TimeoutError(f"PING to {target_ip}:{target_port} timed out after {timeout}s")

    async def send_find_node(
        self,
        target_ip: str,
        target_port: int,
        target_id: NodeID,
        timeout: float = 5.0,
    ) -> List[NodeInfo]:
        """Send FIND_NODE RPC to discover closest nodes to target_id."""
        find_msg = Message(
            type=MessageType.FIND_NODE,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
            sender_tcp_port=self.tcp_port,
            payload={"target_node_id": target_id.hex()},
        )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Message] = loop.create_future()
        self._pending_requests[find_msg.msg_id] = future
        self.send_datagram(find_msg, target_ip, target_port)

        try:
            response = await asyncio.wait_for(future, timeout=timeout)
            nodes_data = response.payload.get("nodes", [])
            discovered_nodes: List[NodeInfo] = []
            for node_dict in nodes_data:
                try:
                    node_info = NodeInfo.from_dict(node_dict)
                    discovered_nodes.append(node_info)
                    self.routing_table.add_contact(node_info)
                except Exception:
                    pass
            return discovered_nodes
        except asyncio.TimeoutError:
            self._pending_requests.pop(find_msg.msg_id, None)
            raise TimeoutError(f"FIND_NODE to {target_ip}:{target_port} timed out after {timeout}s")

    def error_received(self, exc: Exception) -> None:
        logger.error(f"UDP protocol error: {exc}")

    def connection_lost(self, exc: Optional[Exception]) -> None:
        logger.info(f"UDP connection closed: {exc}")


class TCPTaskServer:
    """
    Framed TCP server for remote task dispatching.
    Header: 4-byte big-endian unsigned integer (uint32) payload length.
    """

    HEADER_FORMAT = ">I"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, node_id: NodeID, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self._server: Optional[asyncio.Server] = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        logger.info(f"TCP TaskServer listening on {self.host}:{self.port}")

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("TCP TaskServer stopped")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_addr = writer.get_extra_info("peername")
        try:
            header_bytes = await reader.readexactly(self.HEADER_SIZE)
            (payload_len,) = struct.unpack(self.HEADER_FORMAT, header_bytes)
            task_payload = await reader.readexactly(payload_len)

            task_result = await TaskSerializer.execute_task(task_payload)
            result_json_bytes = json.dumps(task_result.to_dict()).encode("utf-8")
            resp_len_header = struct.pack(self.HEADER_FORMAT, len(result_json_bytes))

            writer.write(resp_len_header + result_json_bytes)
            await writer.drain()

        except asyncio.IncompleteReadError:
            logger.warning(f"Connection dropped by peer {peer_addr}")
        except Exception as e:
            logger.error(f"Error handling task client {peer_addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()


class TCPTaskClient:
    """Async client for submitting tasks to a remote node's TCPTaskServer."""

    HEADER_FORMAT = ">I"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    @classmethod
    async def send_task(cls, host: str, port: int, payload_bytes: bytes, timeout: float = 30.0) -> TaskResult:
        async def _communicator() -> TaskResult:
            reader, writer = await asyncio.open_connection(host, port)
            try:
                header = struct.pack(cls.HEADER_FORMAT, len(payload_bytes))
                writer.write(header + payload_bytes)
                await writer.drain()

                resp_header = await reader.readexactly(cls.HEADER_SIZE)
                (resp_len,) = struct.unpack(cls.HEADER_FORMAT, resp_header)

                resp_json_bytes = await reader.readexactly(resp_len)
                resp_dict = json.loads(resp_json_bytes.decode("utf-8"))
                return TaskResult.from_dict(resp_dict)

            finally:
                writer.close()
                await writer.wait_closed()

        try:
            return await asyncio.wait_for(_communicator(), timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Task submission to {host}:{port} timed out after {timeout}s")
