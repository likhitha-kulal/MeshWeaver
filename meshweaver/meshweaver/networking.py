"""
MeshWeaver Networking Protocol Implementation
Provides UDP protocol (DatagramProtocol) for node discovery, PING-PONG RPCs, GOSSIP heartbeats, and FIND_NODE DHT routing,
along with TCP server/client transport for reliable binary task execution transfer.
"""

import asyncio
import json
import logging
import struct
from typing import Callable, Dict, List, Optional, Tuple

from meshweaver.models import Message, MessageType, NodeID, NodeInfo, TaskResult
from meshweaver.routing_table import RoutingTable
from meshweaver.task_serializer import TaskSerializer

# Set up logger for MeshWeaver networking
logger = logging.getLogger("meshweaver.networking")


class UDPNodeProtocol(asyncio.DatagramProtocol):
    """
    Asyncio DatagramProtocol implementation for MeshWeaver UDP communication.
    Handles ping/pong node discovery, routing table population, GOSSIP, and FIND_NODE RPCs.
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
        logger.info(f"UDP server bound on port {self.local_udp_port} for node {self.node_id}")

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        """Called by asyncio event loop when a UDP datagram is received."""
        try:
            json_str = data.decode("utf-8")
            msg = Message.from_json(json_str)
            logger.info(f"[UDP RECV] {msg.type} from {addr[0]}:{addr[1]} (Sender ID: {msg.sender_id[:8]}..., msg_id={msg.msg_id[:8]})")

            # Update routing table with sender information (Kademlia contact refresh)
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
                logger.warning(f"Failed to update routing table for sender {msg.sender_id}: {e}")

            if msg.type == MessageType.PING:
                self._handle_ping(msg, addr)
            elif msg.type == MessageType.FIND_NODE:
                self._handle_find_node(msg, addr)
            elif msg.type == MessageType.GOSSIP:
                if self.gossip_handler is not None:
                    self.gossip_handler(msg.payload)
            elif msg.msg_id in self._pending_requests:
                # Resolve pending RPC request future
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
            sender_tcp_port=self.tcp_port,
            payload={"status": "OK", "received_ping_id": ping_msg.msg_id},
        )
        self.send_datagram(pong_msg, addr[0], addr[1])
        logger.info(f"[UDP SENT] PONG to {addr[0]}:{addr[1]} (msg_id={pong_msg.msg_id[:8]})")

    def _handle_find_node(self, find_msg: Message, addr: Tuple[str, int]) -> None:
        """
        Handle incoming FIND_NODE RPC request.
        Looks up the closest K nodes to the requested target_node_id in the routing table.
        """
        target_hex = find_msg.payload.get("target_node_id")
        if not target_hex:
            logger.warning(f"Malformed FIND_NODE from {addr}: missing target_node_id")
            return

        try:
            target_id = NodeID(target_hex)
            sender_node_id = NodeID(find_msg.sender_id)
        except Exception as e:
            logger.warning(f"Invalid node ID format in FIND_NODE payload: {e}")
            return

        # Find closest contacts, excluding the requester
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
        logger.info(
            f"[UDP SENT] FIND_NODE_RESPONSE to {addr[0]}:{addr[1]} "
            f"containing {len(closest_contacts)} nodes (msg_id={response_msg.msg_id[:8]})"
        )

    def send_datagram(self, msg: Message, target_ip: str, target_port: int) -> None:
        """Send raw JSON encoded datagram to target endpoint."""
        if not self.transport:
            raise RuntimeError("UDP transport is not connected")
        payload_bytes = msg.to_json().encode("utf-8")
        self.transport.sendto(payload_bytes, (target_ip, target_port))

    def send_gossip(self, target_ip: str, target_port: int, payload: Dict[str, object]) -> None:
        """Broadcast a gossip heartbeat to a known neighbor."""
        gossip_msg = Message(
            type=MessageType.GOSSIP,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
            sender_tcp_port=self.tcp_port,
            payload=payload,
        )
        self.send_datagram(gossip_msg, target_ip, target_port)

    async def send_ping(self, target_ip: str, target_port: int, timeout: float = 5.0) -> Message:
        """
        Send PING to target node and wait for PONG response.
        """
        ping_msg = Message(
            type=MessageType.PING,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.local_udp_port,
            sender_tcp_port=self.tcp_port,
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

    async def send_find_node(
        self,
        target_ip: str,
        target_port: int,
        target_id: NodeID,
        timeout: float = 5.0,
    ) -> List[NodeInfo]:
        """
        Send FIND_NODE RPC to a target node querying for the closest nodes to `target_id`.
        Learns returned contacts and updates the local routing table.
        """
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

        logger.info(
            f"[UDP SENT] FIND_NODE to {target_ip}:{target_port} for target "
            f"{target_id.hex()[:8]}... (msg_id={find_msg.msg_id[:8]})"
        )
        self.send_datagram(find_msg, target_ip, target_port)

        try:
            response = await asyncio.wait_for(future, timeout=timeout)
            nodes_data = response.payload.get("nodes", [])
            discovered_nodes: List[NodeInfo] = []
            for node_dict in nodes_data:
                try:
                    node_info = NodeInfo.from_dict(node_dict)
                    discovered_nodes.append(node_info)
                    # Automatically populate local routing table with discovered peers
                    self.routing_table.add_contact(node_info)
                except Exception as e:
                    logger.warning(f"Error parsing returned NodeInfo dict: {e}")
            return discovered_nodes
        except asyncio.TimeoutError:
            self._pending_requests.pop(find_msg.msg_id, None)
            logger.warning(f"[UDP TIMEOUT] FIND_NODE to {target_ip}:{target_port} timed out after {timeout}s")
            raise TimeoutError(f"FIND_NODE to {target_ip}:{target_port} timed out after {timeout}s")

    def error_received(self, exc: Exception) -> None:
        logger.error(f"[UDP ERROR] Protocol error: {exc}")

    def connection_lost(self, exc: Optional[Exception]) -> None:
        logger.info(f"UDP connection closed: {exc}")


class TCPTaskServer:
    """
    Length-prefixed streaming TCP server for receiving serialized task execution payloads.
    Framing: 4 bytes big-endian unsigned integer (uint32) length header + payload.
    """

    HEADER_FORMAT = ">I"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, node_id: NodeID, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self._server: Optional[asyncio.Server] = None

    async def start(self) -> None:
        """Start TCP server on specified host/port."""
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        bound_port = self._server.sockets[0].getsockname()[1]
        self.port = bound_port
        logger.info(f"TCP TaskServer listening on {self.host}:{self.port}")

    async def stop(self) -> None:
        """Stop TCP server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("TCP TaskServer stopped")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_addr = writer.get_extra_info("peername")
        logger.info(f"[TCP RECV] Connection established from {peer_addr}")

        try:
            # Read 4-byte header
            header_bytes = await reader.readexactly(self.HEADER_SIZE)
            (payload_len,) = struct.unpack(self.HEADER_FORMAT, header_bytes)

            logger.info(f"[TCP RECV] Reading {payload_len} bytes task payload from {peer_addr}")
            task_payload = await reader.readexactly(payload_len)

            # Verify payload integrity by checking TaskEnvelope hash before deserializing
            task_result = await self._verify_and_execute(task_payload)

            # Serialize result to JSON string format
            result_json_bytes = json.dumps(task_result.to_dict()).encode("utf-8")
            resp_len_header = struct.pack(self.HEADER_FORMAT, len(result_json_bytes))

            writer.write(resp_len_header + result_json_bytes)
            await writer.drain()

            status_str = "SUCCESS" if task_result.success else f"FAILED ({task_result.error_type})"
            logger.info(f"[TCP SENT] Execution result sent to {peer_addr} - Status: {status_str}")

        except asyncio.IncompleteReadError:
            logger.warning(f"[TCP WARN] Connection closed prematurely by {peer_addr}")
        except Exception as e:
            logger.error(f"[TCP ERROR] Exception handling client {peer_addr}: {e}", exc_info=True)
        finally:
            writer.close()
            await writer.wait_closed()

    async def _verify_and_execute(self, task_payload: bytes) -> TaskResult:
        """
        Verify TaskEnvelope integrity before executing.
        Returns IntegrityError TaskResult if hash verification fails.
        """
        try:
            from meshweaver.models import TaskEnvelope
            
            # Try parsing as JSON TaskEnvelope
            try:
                envelope_dict = json.loads(task_payload.decode("utf-8"))
                if isinstance(envelope_dict, dict) and "sha256" in envelope_dict and "payload" in envelope_dict:
                    envelope = TaskEnvelope.from_dict(envelope_dict)
                    if not envelope.verify():
                        logger.warning("[TCP INTEGRITY] TaskEnvelope hash verification failed")
                        return TaskResult(
                            task_id="unknown",
                            success=False,
                            error_type="IntegrityError",
                            error_message="Task payload hash verification failed - possible corruption or tampering",
                        )
                    # Pass unwrapped binary payload
                    return await TaskSerializer.execute_task(envelope.payload)
            except (UnicodeDecodeError, json.JSONDecodeError):
                pass
            
            # Direct binary cloudpickle payload fallback
            return await TaskSerializer.execute_task(task_payload)

        except Exception as e:
            logger.error(f"[TCP INTEGRITY] Unexpected error during verification/execution: {e}", exc_info=True)
            return TaskResult(
                task_id="unknown",
                success=False,
                error_type="IntegrityError",
                error_message=f"Payload verification error: {str(e)}",
            )


class TCPTaskClient:
    """
    TCP Client for submitting cloudpickled tasks to a remote node's TaskServer.
    """

    HEADER_FORMAT = ">I"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    @classmethod
    async def send_task(cls, host: str, port: int, payload_bytes: bytes, timeout: float = 30.0) -> TaskResult:
        """
        Connect to target TCPTaskServer, send serialized payload, and receive TaskResult.
        """
        logger.info(f"[TCP CLIENT] Connecting to {host}:{port} to send {len(payload_bytes)} task bytes")

        async def _communicator() -> TaskResult:
            reader, writer = await asyncio.open_connection(host, port)
            try:
                # Send payload length header + payload bytes
                header = struct.pack(cls.HEADER_FORMAT, len(payload_bytes))
                writer.write(header + payload_bytes)
                await writer.drain()

                # Read response length header
                resp_header = await reader.readexactly(cls.HEADER_SIZE)
                (resp_len,) = struct.unpack(cls.HEADER_FORMAT, resp_header)

                # Read response payload
                resp_json_bytes = await reader.readexactly(resp_len)
                resp_dict = json.loads(resp_json_bytes.decode("utf-8"))

                return TaskResult.from_dict(resp_dict)

            finally:
                writer.close()
                await writer.wait_closed()

        try:
            return await asyncio.wait_for(_communicator(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.error(f"[TCP CLIENT TIMEOUT] Task submission to {host}:{port} timed out after {timeout}s")
            raise TimeoutError(f"Task submission to {host}:{port} timed out after {timeout}s")
