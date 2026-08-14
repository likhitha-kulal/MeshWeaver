"""
MeshWeaver Networking Protocol (Execution & Reliability Track - Person B / Likhitha)
Provides TCPTaskServer and TCPTaskClient for reliable binary task transmission and execution over streaming sockets.
"""

import asyncio
import json
import logging
import struct
from typing import Optional

from meshweaver.models import TaskResult
from meshweaver.task_serializer import TaskSerializer

logger = logging.getLogger("meshweaver.networking")


class TCPTaskServer:
    """
    Length-prefixed streaming TCP server for receiving serialized task execution payloads.
    Framing: 4 bytes big-endian unsigned integer (uint32) length header + payload.
    """

    HEADER_FORMAT = ">I"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, host: str = "127.0.0.1", port: int = 9001):
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
            header_bytes = await reader.readexactly(self.HEADER_SIZE)
            (payload_len,) = struct.unpack(self.HEADER_FORMAT, header_bytes)

            logger.info(f"[TCP RECV] Reading {payload_len} bytes task payload from {peer_addr}")
            task_payload = await reader.readexactly(payload_len)

            task_result = await TaskSerializer.execute_task(task_payload)

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
            logger.error(f"[TCP CLIENT TIMEOUT] Task submission to {host}:{port} timed out after {timeout}s")
            raise TimeoutError(f"Task submission to {host}:{port} timed out after {timeout}s")
