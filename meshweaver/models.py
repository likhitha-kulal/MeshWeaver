"""
MeshWeaver Data Models
Core data structures for node identification, routing, messaging, and task execution.
"""

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import os
import time
from typing import Any, Dict, Optional, Union
import uuid


class NodeID:
    """
    160-bit Kademlia-compatible node identifier.
    Stored internally as a 20-byte payload with XOR distance metric calculations.
    """

    ID_BIT_LENGTH = 160
    ID_BYTE_LENGTH = 20

    def __init__(self, value: Optional[Union[bytes, int, str]] = None):
        if value is None:
            self._bytes = os.urandom(self.ID_BYTE_LENGTH)
        elif isinstance(value, bytes):
            if len(value) != self.ID_BYTE_LENGTH:
                raise ValueError(f"NodeID bytes must be exactly {self.ID_BYTE_LENGTH} bytes long")
            self._bytes = value
        elif isinstance(value, int):
            self._bytes = value.to_bytes(self.ID_BYTE_LENGTH, byteorder="big")
        elif isinstance(value, str):
            clean_hex = value.strip().lower()
            if len(clean_hex) != self.ID_BYTE_LENGTH * 2:
                raise ValueError(f"Hex NodeID string must be {self.ID_BYTE_LENGTH * 2} characters")
            self._bytes = bytes.fromhex(clean_hex)
        else:
            raise TypeError("NodeID value must be bytes, int, hex string, or None")

    @classmethod
    def from_string_hash(cls, source: str) -> "NodeID":
        """Generate a NodeID from the SHA-1 hash of a string."""
        digest = hashlib.sha1(source.encode("utf-8")).digest()
        return cls(digest)

    @property
    def bytes(self) -> bytes:
        return self._bytes

    @property
    def int(self) -> int:
        return int.from_bytes(self._bytes, byteorder="big")

    def hex(self) -> str:
        return self._bytes.hex()

    def distance(self, other: "NodeID") -> int:
        """Calculate XOR distance metric between two NodeIDs."""
        return self.int ^ other.int

    def __eq__(self, other: object) -> bool:
        if isinstance(other, NodeInfo):
            return self._bytes == other.node_id._bytes
        if not isinstance(other, NodeID):
            return False
        return self._bytes == other._bytes

    def __hash__(self) -> int:
        return hash(self._bytes)

    def __repr__(self) -> str:
        return f"NodeID({self.hex()[:8]}...)"

    def __str__(self) -> str:
        return self.hex()


@dataclass
class NodeInfo:
    """Network contact metadata for a peer node."""
    node_id: NodeID
    ip: str
    udp_port: int
    tcp_port: Optional[int] = None
    last_seen: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "node_id": self.node_id.hex(),
            "ip": self.ip,
            "udp_port": self.udp_port,
            "last_seen": self.last_seen,
        }
        if self.tcp_port is not None:
            data["tcp_port"] = self.tcp_port
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeInfo":
        return cls(
            node_id=NodeID(data["node_id"]),
            ip=data["ip"],
            udp_port=int(data["udp_port"]),
            tcp_port=int(data["tcp_port"]) if data.get("tcp_port") is not None else None,
            last_seen=float(data.get("last_seen", time.time())),
        )

    def __eq__(self, other: object) -> bool:
        if isinstance(other, NodeID):
            return self.node_id == other
        if not isinstance(other, NodeInfo):
            return False
        return self.node_id == other.node_id

    def __hash__(self) -> int:
        return hash(self.node_id)


class MessageType(str, Enum):
    """RPC and network control message types."""
    PING = "PING"
    PONG = "PONG"
    FIND_NODE = "FIND_NODE"
    FIND_NODE_RESPONSE = "FIND_NODE_RESPONSE"
    GOSSIP = "GOSSIP"
    TASK_EXECUTE = "TASK_EXECUTE"
    TASK_RESULT = "TASK_RESULT"
    ERROR = "ERROR"


@dataclass
class Message:
    """Network datagram message container."""
    type: MessageType
    sender_id: str
    sender_udp_port: int
    sender_tcp_port: int = 0
    msg_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "msg_id": self.msg_id,
            "type": self.type.value if isinstance(self.type, MessageType) else str(self.type),
            "sender_id": self.sender_id,
            "sender_udp_port": self.sender_udp_port,
            "sender_tcp_port": self.sender_tcp_port,
            "payload": self.payload,
            "timestamp": self.timestamp,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        return cls(
            msg_id=data.get("msg_id", str(uuid.uuid4())),
            type=MessageType(data["type"]),
            sender_id=data["sender_id"],
            sender_udp_port=int(data["sender_udp_port"]),
            sender_tcp_port=int(data.get("sender_tcp_port", 0)),
            payload=data.get("payload", {}),
            timestamp=float(data.get("timestamp", time.time())),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "Message":
        return cls.from_dict(json.loads(json_str))


@dataclass
class TaskEnvelope:
    """
    Integrity envelope wrapping serialized task payloads with SHA-256 checksums.
    Prevents execution of corrupted or tampered code.
    """
    payload: bytes
    sha256: str

    @classmethod
    def wrap(cls, payload: bytes) -> "TaskEnvelope":
        """Compute SHA-256 checksum and package into envelope."""
        return cls(payload=payload, sha256=hashlib.sha256(payload).hexdigest())

    def verify(self) -> bool:
        """Verify checksum integrity against payload."""
        return hashlib.sha256(self.payload).hexdigest() == self.sha256

    def to_dict(self) -> Dict[str, Any]:
        return {
            "payload": self.payload.hex(),
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskEnvelope":
        return cls(
            payload=bytes.fromhex(data["payload"]),
            sha256=data["sha256"],
        )


@dataclass
class TaskResult:
    """Encapsulates output, status, and error diagnostics from a task execution."""
    task_id: str
    success: bool
    result_bytes: Optional[bytes] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    traceback: Optional[str] = None
    payload_hash: Optional[str] = None

    def __post_init__(self) -> None:
        if self.result_bytes is not None and self.payload_hash is None:
            self.payload_hash = self._compute_hash(self.result_bytes)

    @staticmethod
    def _compute_hash(payload: bytes) -> str:
        return hashlib.sha256(payload).hexdigest()

    def verify_payload(self) -> bool:
        if self.result_bytes is None:
            return self.payload_hash in (None, "")
        return self.payload_hash == self._compute_hash(self.result_bytes)

    def to_dict(self) -> Dict[str, Any]:
        if self.result_bytes is not None and self.payload_hash is None:
            self.payload_hash = self._compute_hash(self.result_bytes)
        return {
            "task_id": self.task_id,
            "success": self.success,
            "result_bytes": self.result_bytes.hex() if self.result_bytes else None,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "traceback": self.traceback,
            "payload_hash": self.payload_hash,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskResult":
        res_bytes = bytes.fromhex(data["result_bytes"]) if data.get("result_bytes") else None
        result = cls(
            task_id=data["task_id"],
            success=data["success"],
            result_bytes=res_bytes,
            error_type=data.get("error_type"),
            error_message=data.get("error_message"),
            traceback=data.get("traceback"),
            payload_hash=data.get("payload_hash"),
        )
        if result.result_bytes is not None and result.payload_hash is None:
            result.payload_hash = result._compute_hash(result.result_bytes)
        return result


class PeerStatus(str, Enum):
    """Node lifecycle and health status."""
    ALIVE = "ALIVE"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"
