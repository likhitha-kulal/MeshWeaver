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
from typing import Any, Dict, List, Optional, Union
import uuid
import zlib


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


class ElectionRole(str, Enum):
    """Consensus roles in distributed leader election."""
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class RaftCommandType(str, Enum):
    """Command types executed on the replicated state machine."""
    SET = "SET"
    GET = "GET"
    DELETE = "DELETE"
    CAS = "CAS"
    INCREMENT = "INCREMENT"
    LOCK_ACQUIRE = "LOCK_ACQUIRE"
    LOCK_RELEASE = "LOCK_RELEASE"
    BATCH = "BATCH"
    NOOP = "NOOP"
    # Week 4 Day 3: Consensus Job Orchestration & Membership
    JOB_SUBMIT = "JOB_SUBMIT"
    JOB_ASSIGN = "JOB_ASSIGN"
    JOB_COMPLETE = "JOB_COMPLETE"
    JOB_FAIL = "JOB_FAIL"
    JOB_CANCEL = "JOB_CANCEL"
    MEMBERSHIP_CHANGE = "MEMBERSHIP_CHANGE"
    # Week 4 Day 4: 2PC Distributed Transactions & Synchronization Primitives
    TX_PREPARE = "TX_PREPARE"
    TX_COMMIT = "TX_COMMIT"
    TX_ABORT = "TX_ABORT"
    BARRIER_REGISTER = "BARRIER_REGISTER"
    BARRIER_ENTER = "BARRIER_ENTER"
    SEMAPHORE_ACQUIRE = "SEMAPHORE_ACQUIRE"
    SEMAPHORE_RELEASE = "SEMAPHORE_RELEASE"




@dataclass
class LogEntry:
    """
    Term-indexed persistent entry in the Raft replicated commit log.
    Maintains deterministic 1-based index numbering and execution metadata.
    """
    index: int
    term: int
    command_type: RaftCommandType = RaftCommandType.NOOP
    key: Optional[str] = None
    value: Optional[Any] = None
    client_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    fencing_token: Optional[int] = None
    extra_data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "term": self.term,
            "command_type": self.command_type.value if isinstance(self.command_type, RaftCommandType) else str(self.command_type),
            "key": self.key,
            "value": self.value,
            "client_id": self.client_id,
            "timestamp": self.timestamp,
            "fencing_token": self.fencing_token,
            "extra_data": self.extra_data,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEntry":
        cmd_type = data.get("command_type", RaftCommandType.NOOP.value)
        if isinstance(cmd_type, str):
            try:
                cmd_type = RaftCommandType(cmd_type)
            except ValueError:
                cmd_type = RaftCommandType.NOOP
        return cls(
            index=int(data["index"]),
            term=int(data["term"]),
            command_type=cmd_type,
            key=data.get("key"),
            value=data.get("value"),
            client_id=data.get("client_id"),
            timestamp=float(data.get("timestamp", time.time())),
            fencing_token=int(data["fencing_token"]) if data.get("fencing_token") is not None else None,
            extra_data=data.get("extra_data", {}),
        )




@dataclass
class AppendEntriesRequest:
    """
    Raft RPC request sent by leader to replicate log entries and serve as heartbeat.
    Enforces log matching invariant via prev_log_index and prev_log_term.
    """
    term: int
    leader_id: str
    prev_log_index: int = 0
    prev_log_term: int = 0
    entries: List[LogEntry] = field(default_factory=list)
    leader_commit: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "leader_id": self.leader_id,
            "prev_log_index": self.prev_log_index,
            "prev_log_term": self.prev_log_term,
            "entries": [e.to_dict() for e in self.entries],
            "leader_commit": self.leader_commit,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AppendEntriesRequest":
        raw_entries = data.get("entries", [])
        parsed_entries = [LogEntry.from_dict(e) if isinstance(e, dict) else e for e in raw_entries]
        return cls(
            term=int(data["term"]),
            leader_id=data["leader_id"],
            prev_log_index=int(data.get("prev_log_index", 0)),
            prev_log_term=int(data.get("prev_log_term", 0)),
            entries=parsed_entries,
            leader_commit=int(data.get("leader_commit", 0)),
        )




@dataclass
class AppendEntriesResponse:
    """
    Raft RPC response returned by follower acknowledging or rejecting log append.
    """
    term: int
    follower_id: str
    success: bool
    match_index: int = 0
    last_log_index: int = 0
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "follower_id": self.follower_id,
            "success": self.success,
            "match_index": self.match_index,
            "last_log_index": self.last_log_index,
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AppendEntriesResponse":
        return cls(
            term=int(data["term"]),
            follower_id=data["follower_id"],
            success=bool(data["success"]),
            match_index=int(data.get("match_index", 0)),
            last_log_index=int(data.get("last_log_index", 0)),
            error_message=data.get("error_message"),
        )




@dataclass
class InstallSnapshotRequest:
    """
    Raft InstallSnapshot RPC payload sent by leader when follower lags behind compacted snapshot.
    """
    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    data: Dict[str, Any] = field(default_factory=dict)
    done: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "leader_id": self.leader_id,
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "data": self.data,
            "done": self.done,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "InstallSnapshotRequest":
        return cls(
            term=int(data["term"]),
            leader_id=data["leader_id"],
            last_included_index=int(data["last_included_index"]),
            last_included_term=int(data["last_included_term"]),
            data=data.get("data", {}),
            done=bool(data.get("done", True)),
        )


@dataclass
class InstallSnapshotResponse:
    """
    Raft InstallSnapshot RPC response returned by follower acknowledging snapshot restore.
    """
    term: int
    follower_id: str
    success: bool
    match_index: int = 0
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "follower_id": self.follower_id,
            "success": self.success,
            "match_index": self.match_index,
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "InstallSnapshotResponse":
        return cls(
            term=int(data["term"]),
            follower_id=data["follower_id"],
            success=bool(data["success"]),
            match_index=int(data.get("match_index", 0)),
            error_message=data.get("error_message"),
        )


@dataclass
class DistributedLock:
    """
    Distributed mutual exclusion lease with monotonic fencing tokens.
    Guarantees safety against zombie leaders or slow network partitions.
    """
    resource: str
    holder_id: str
    fencing_token: int
    acquired_at: float = field(default_factory=time.time)
    ttl_seconds: float = 30.0

    def is_expired(self, now: Optional[float] = None) -> bool:
        current_ts = now if now is not None else time.time()
        return current_ts > (self.acquired_at + self.ttl_seconds)

    def remaining_ttl(self, now: Optional[float] = None) -> float:
        current_ts = now if now is not None else time.time()
        return max(0.0, (self.acquired_at + self.ttl_seconds) - current_ts)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "resource": self.resource,
            "holder_id": self.holder_id,
            "fencing_token": self.fencing_token,
            "acquired_at": self.acquired_at,
            "ttl_seconds": self.ttl_seconds,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DistributedLock":
        return cls(
            resource=data["resource"],
            holder_id=data["holder_id"],
            fencing_token=int(data["fencing_token"]),
            acquired_at=float(data.get("acquired_at", time.time())),
            ttl_seconds=float(data.get("ttl_seconds", 30.0)),
        )


@dataclass
class LockAcquireResult:
    """Result returned from an acquire_lock request."""
    acquired: bool
    resource: str
    fencing_token: Optional[int] = None
    holder_id: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "acquired": self.acquired,
            "resource": self.resource,
            "fencing_token": self.fencing_token,
            "holder_id": self.holder_id,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LockAcquireResult":
        return cls(
            acquired=bool(data["acquired"]),
            resource=data["resource"],
            fencing_token=int(data["fencing_token"]) if data.get("fencing_token") is not None else None,
            holder_id=data.get("holder_id"),
            error=data.get("error"),
        )


class MessageType(str, Enum):
    """RPC and network control message types."""
    PING = "PING"
    PONG = "PONG"
    FIND_NODE = "FIND_NODE"
    FIND_NODE_RESPONSE = "FIND_NODE_RESPONSE"
    STORE = "STORE"
    STORE_RESPONSE = "STORE_RESPONSE"
    FIND_VALUE = "FIND_VALUE"
    FIND_VALUE_RESPONSE = "FIND_VALUE_RESPONSE"
    GOSSIP = "GOSSIP"
    TASK_EXECUTE = "TASK_EXECUTE"
    TASK_RESULT = "TASK_RESULT"
    ERROR = "ERROR"
    # Distributed Consensus / Leader Election Messages
    ELECTION_VOTE_REQUEST = "ELECTION_VOTE_REQUEST"
    ELECTION_VOTE_RESPONSE = "ELECTION_VOTE_RESPONSE"
    LEADER_HEARTBEAT = "LEADER_HEARTBEAT"
    LEADER_HEARTBEAT_ACK = "LEADER_HEARTBEAT_ACK"
    # Raft Replicated Log & State Machine Messages
    RAFT_APPEND_ENTRIES_REQUEST = "RAFT_APPEND_ENTRIES_REQUEST"
    RAFT_APPEND_ENTRIES_RESPONSE = "RAFT_APPEND_ENTRIES_RESPONSE"
    RAFT_INSTALL_SNAPSHOT_REQUEST = "RAFT_INSTALL_SNAPSHOT_REQUEST"
    RAFT_INSTALL_SNAPSHOT_RESPONSE = "RAFT_INSTALL_SNAPSHOT_RESPONSE"
    RAFT_PROPOSE_COMMAND = "RAFT_PROPOSE_COMMAND"
    RAFT_PROPOSE_RESPONSE = "RAFT_PROPOSE_RESPONSE"
    RAFT_SYNC_STATE = "RAFT_SYNC_STATE"
    RAFT_SYNC_STATE_RESPONSE = "RAFT_SYNC_STATE_RESPONSE"
    # Week 4 Day 4: 2PC Distributed Transactions & Distributed Barrier RPCs
    TX_PREPARE_REQUEST = "TX_PREPARE_REQUEST"
    TX_PREPARE_RESPONSE = "TX_PREPARE_RESPONSE"
    TX_COMMIT_REQUEST = "TX_COMMIT_REQUEST"
    TX_COMMIT_RESPONSE = "TX_COMMIT_RESPONSE"
    TX_ABORT_REQUEST = "TX_ABORT_REQUEST"
    TX_ABORT_RESPONSE = "TX_ABORT_RESPONSE"
    BARRIER_SYNC_REQUEST = "BARRIER_SYNC_REQUEST"
    BARRIER_SYNC_RESPONSE = "BARRIER_SYNC_RESPONSE"



@dataclass
class VoteRequest:
    """RPC payload sent by candidates requesting peer votes."""
    term: int
    candidate_id: str
    last_log_index: int = 0
    last_log_term: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "candidate_id": self.candidate_id,
            "last_log_index": self.last_log_index,
            "last_log_term": self.last_log_term,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VoteRequest":
        return cls(
            term=int(data["term"]),
            candidate_id=data["candidate_id"],
            last_log_index=int(data.get("last_log_index", 0)),
            last_log_term=int(data.get("last_log_term", 0)),
        )


@dataclass
class VoteResponse:
    """RPC payload returned by peers responding to vote requests."""
    term: int
    vote_granted: bool
    voter_id: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "vote_granted": self.vote_granted,
            "voter_id": self.voter_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VoteResponse":
        return cls(
            term=int(data["term"]),
            vote_granted=bool(data["vote_granted"]),
            voter_id=data["voter_id"],
        )


@dataclass
class LeaderHeartbeat:
    """Heartbeat lease message broadcast periodically by the active leader."""
    term: int
    leader_id: str
    lease_duration: float
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "leader_id": self.leader_id,
            "lease_duration": self.lease_duration,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LeaderHeartbeat":
        return cls(
            term=int(data["term"]),
            leader_id=data["leader_id"],
            lease_duration=float(data["lease_duration"]),
            timestamp=float(data.get("timestamp", time.time())),
        )


@dataclass
class LeaderHeartbeatAck:
    """Follower acknowledgment confirming receipt of leader heartbeat lease."""
    term: int
    node_id: str
    accepted: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "node_id": self.node_id,
            "accepted": self.accepted,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LeaderHeartbeatAck":
        return cls(
            term=int(data["term"]),
            node_id=data["node_id"],
            accepted=bool(data["accepted"]),
        )


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


class ConsensusJobStatus(str, Enum):
    """Execution status for consensus-replicated distributed jobs."""
    SUBMITTED = "SUBMITTED"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class ConsensusJob:
    """
    Consensus-replicated cluster job descriptor.
    Guarantees exactly-once dispatch, fencing token safety, and automatic orphan recovery.
    """
    job_id: str
    func_bytes: Optional[str] = None       # Hex-encoded cloudpickle payload
    args_bytes: Optional[str] = None       # Hex-encoded arguments tuple
    kwargs_bytes: Optional[str] = None     # Hex-encoded kwargs dict
    priority: int = 2                     # QoS priority tier (0=CRITICAL, 2=NORMAL, 4=BACKGROUND)
    status: ConsensusJobStatus = ConsensusJobStatus.SUBMITTED
    submitted_by: str = ""
    assigned_to: Optional[str] = None
    assigned_at: Optional[float] = None
    timeout_seconds: float = 60.0
    retry_count: int = 0
    max_retries: int = 3
    fencing_token: Optional[int] = None
    result_bytes: Optional[str] = None     # Hex-encoded serialized result or None
    error_message: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None

    def is_expired(self, now: Optional[float] = None) -> bool:
        if self.status not in (ConsensusJobStatus.ASSIGNED, ConsensusJobStatus.RUNNING):
            return False
        if self.assigned_at is None:
            return False
        current_ts = now if now is not None else time.time()
        return current_ts > (self.assigned_at + self.timeout_seconds)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "func_bytes": self.func_bytes,
            "args_bytes": self.args_bytes,
            "kwargs_bytes": self.kwargs_bytes,
            "priority": self.priority,
            "status": self.status.value if isinstance(self.status, ConsensusJobStatus) else str(self.status),
            "submitted_by": self.submitted_by,
            "assigned_to": self.assigned_to,
            "assigned_at": self.assigned_at,
            "timeout_seconds": self.timeout_seconds,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "fencing_token": self.fencing_token,
            "result_bytes": self.result_bytes,
            "error_message": self.error_message,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConsensusJob":
        return cls(
            job_id=data["job_id"],
            func_bytes=data.get("func_bytes"),
            args_bytes=data.get("args_bytes"),
            kwargs_bytes=data.get("kwargs_bytes"),
            priority=int(data.get("priority", 2)),
            status=ConsensusJobStatus(data.get("status", ConsensusJobStatus.SUBMITTED.value)),
            submitted_by=data.get("submitted_by", ""),
            assigned_to=data.get("assigned_to"),
            assigned_at=float(data["assigned_at"]) if data.get("assigned_at") is not None else None,
            timeout_seconds=float(data.get("timeout_seconds", 60.0)),
            retry_count=int(data.get("retry_count", 0)),
            max_retries=int(data.get("max_retries", 3)),
            fencing_token=int(data["fencing_token"]) if data.get("fencing_token") is not None else None,
            result_bytes=data.get("result_bytes"),
            error_message=data.get("error_message"),
            created_at=float(data.get("created_at", time.time())),
            completed_at=float(data["completed_at"]) if data.get("completed_at") is not None else None,
        )


class WALRecordType(str, Enum):
    """Types of records logged in the Write-Ahead Log (WAL)."""
    ENTRY = "ENTRY"                      # Regular Raft/state machine log entry
    SNAPSHOT_POINTER = "SNAPSHOT_POINTER"# Pointer to compacted snapshot file on disk
    COMMIT_MARKER = "COMMIT_MARKER"      # Explicit commit index barrier marker
    TX_MARKER = "TX_MARKER"              # 2PC Transaction prepare/commit/abort marker
    CHECKPOINT = "CHECKPOINT"            # Full sync checkpoint marker


class FsyncMode(str, Enum):
    """Disk flush (fsync) durability policies for WAL storage."""
    ALWAYS = "ALWAYS"        # fsync on every single write (maximum durability, lowest throughput)
    PERIODIC = "PERIODIC"    # fsync in background at fixed time intervals (balanced)
    BATCH = "BATCH"          # fsync after N pending records or batch commits (high throughput)
    OFF = "OFF"              # Rely on OS disk buffer cache (fastest, memory-safe)


@dataclass
class StorageConfig:
    """Configuration for persistent disk storage, WAL segments, and snapshotting."""
    data_dir: str = ".mesh_data"
    node_storage_id: str = "node_default"
    max_segment_size_bytes: int = 10 * 1024 * 1024  # 10 MB per segment file
    fsync_mode: FsyncMode = FsyncMode.PERIODIC
    fsync_interval_seconds: float = 0.5
    snapshot_interval_entries: int = 1000
    max_snapshots_retained: int = 3
    wal_cleanup_retention_segments: int = 5
    enable_wal: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "data_dir": self.data_dir,
            "node_storage_id": self.node_storage_id,
            "max_segment_size_bytes": self.max_segment_size_bytes,
            "fsync_mode": self.fsync_mode.value if isinstance(self.fsync_mode, FsyncMode) else str(self.fsync_mode),
            "fsync_interval_seconds": self.fsync_interval_seconds,
            "snapshot_interval_entries": self.snapshot_interval_entries,
            "max_snapshots_retained": self.max_snapshots_retained,
            "wal_cleanup_retention_segments": self.wal_cleanup_retention_segments,
            "enable_wal": self.enable_wal,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StorageConfig":
        return cls(
            data_dir=data.get("data_dir", ".mesh_data"),
            node_storage_id=data.get("node_storage_id", "node_default"),
            max_segment_size_bytes=int(data.get("max_segment_size_bytes", 10 * 1024 * 1024)),
            fsync_mode=FsyncMode(data.get("fsync_mode", FsyncMode.PERIODIC.value)),
            fsync_interval_seconds=float(data.get("fsync_interval_seconds", 0.5)),
            snapshot_interval_entries=int(data.get("snapshot_interval_entries", 1000)),
            max_snapshots_retained=int(data.get("max_snapshots_retained", 3)),
            wal_cleanup_retention_segments=int(data.get("wal_cleanup_retention_segments", 5)),
            enable_wal=bool(data.get("enable_wal", True)),
        )


@dataclass
class WALRecord:
    """
    Append-only record stored in WAL segment files.
    Includes monotonic sequence number, record type, JSON payload, timestamp, and CRC32 checksum.
    """
    seq_no: int
    record_type: WALRecordType
    payload: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    crc32: int = 0

    def __post_init__(self) -> None:
        if self.crc32 == 0:
            self.crc32 = self.compute_crc32()

    def compute_crc32(self) -> int:
        """Calculate CRC32 checksum across seq_no, record_type, payload JSON, and timestamp."""
        payload_json = json.dumps(self.payload, sort_keys=True)
        type_val = self.record_type.value if isinstance(self.record_type, WALRecordType) else str(self.record_type)
        raw = f"{self.seq_no}:{type_val}:{self.timestamp:.6f}:{payload_json}".encode("utf-8")
        return zlib.crc32(raw) & 0xFFFFFFFF

    def verify_crc32(self) -> bool:
        """Verify the stored checksum against computed CRC32."""
        return self.crc32 == self.compute_crc32()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "seq_no": self.seq_no,
            "record_type": self.record_type.value if isinstance(self.record_type, WALRecordType) else str(self.record_type),
            "payload": self.payload,
            "timestamp": self.timestamp,
            "crc32": self.crc32,
        }

    def to_json_line(self) -> str:
        """Serialize record to single-line JSON string with newline for segment file append."""
        return json.dumps(self.to_dict()) + "\n"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WALRecord":
        return cls(
            seq_no=int(data["seq_no"]),
            record_type=WALRecordType(data["record_type"]),
            payload=data["payload"],
            timestamp=float(data.get("timestamp", time.time())),
            crc32=int(data.get("crc32", 0)),
        )

    @classmethod
    def from_json_line(cls, line: str) -> "WALRecord":
        return cls.from_dict(json.loads(line.strip()))


class TxStatus(str, Enum):
    """Lifecycle status for distributed Two-Phase Commit (2PC) transactions."""
    ACTIVE = "ACTIVE"          # Transaction initialized, read/write ops being buffered
    PREPARING = "PREPARING"    # Coordinator dispatched PREPARE RPCs to participants
    PREPARED = "PREPARED"      # All participants voted YES, locks/leases guaranteed
    COMMITTING = "COMMITTING"  # Coordinator dispatched COMMIT RPCs
    COMMITTED = "COMMITTED"    # All operations applied atomically and locks released
    ABORTING = "ABORTING"      # Coordinator dispatched ABORT RPCs due to conflict/timeout
    ABORTED = "ABORTED"        # Rollback executed and locks released
    TIMED_OUT = "TIMED_OUT"    # Transaction lease expired prior to commit


class TxIsolationLevel(str, Enum):
    """Transaction isolation semantics."""
    READ_COMMITTED = "READ_COMMITTED"
    REPEATABLE_READ = "REPEATABLE_READ"
    SERIALIZABLE = "SERIALIZABLE"


class TxOperationType(str, Enum):
    """Operation types within an atomic transaction."""
    SET = "SET"
    DELETE = "DELETE"
    INCREMENT = "INCREMENT"


@dataclass
class TxOperation:
    """Individual state mutation within a distributed transaction."""
    op_type: TxOperationType
    key: str
    value: Optional[Any] = None
    delta: int = 1
    expected_version: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "op_type": self.op_type.value if isinstance(self.op_type, TxOperationType) else str(self.op_type),
            "key": self.key,
            "value": self.value,
            "delta": self.delta,
            "expected_version": self.expected_version,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TxOperation":
        return cls(
            op_type=TxOperationType(data["op_type"]),
            key=data["key"],
            value=data.get("value"),
            delta=int(data.get("delta", 1)),
            expected_version=int(data["expected_version"]) if data.get("expected_version") is not None else None,
        )


@dataclass
class TxPrepareResult:
    """Participant response to a 2PC PREPARE request."""
    tx_id: str
    participant_id: str
    vote_yes: bool
    error_message: Optional[str] = None
    fencing_tokens: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tx_id": self.tx_id,
            "participant_id": self.participant_id,
            "vote_yes": self.vote_yes,
            "error_message": self.error_message,
            "fencing_tokens": self.fencing_tokens,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TxPrepareResult":
        return cls(
            tx_id=data["tx_id"],
            participant_id=data["participant_id"],
            vote_yes=bool(data["vote_yes"]),
            error_message=data.get("error_message"),
            fencing_tokens=data.get("fencing_tokens", {}),
        )


@dataclass
class TxRecord:
    """
    Complete descriptor of a distributed 2PC transaction.
    Maintains read/write sets, operations, participating nodes, fencing tokens, and execution timeouts.
    """
    tx_id: str
    coordinator_id: str
    status: TxStatus = TxStatus.ACTIVE
    isolation_level: TxIsolationLevel = TxIsolationLevel.SERIALIZABLE
    operations: List[TxOperation] = field(default_factory=list)
    read_set: Dict[str, Any] = field(default_factory=dict)
    write_set: Dict[str, Any] = field(default_factory=dict)
    participants: List[str] = field(default_factory=list)
    prepared_participants: List[str] = field(default_factory=list)
    fencing_tokens: Dict[str, int] = field(default_factory=dict)
    timeout_seconds: float = 10.0
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    error_message: Optional[str] = None

    def is_expired(self, now: Optional[float] = None) -> bool:
        if self.status in (TxStatus.COMMITTED, TxStatus.ABORTED):
            return False
        current_ts = now if now is not None else time.time()
        return current_ts > (self.created_at + self.timeout_seconds)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tx_id": self.tx_id,
            "coordinator_id": self.coordinator_id,
            "status": self.status.value if isinstance(self.status, TxStatus) else str(self.status),
            "isolation_level": self.isolation_level.value if isinstance(self.isolation_level, TxIsolationLevel) else str(self.isolation_level),
            "operations": [op.to_dict() for op in self.operations],
            "read_set": self.read_set,
            "write_set": self.write_set,
            "participants": self.participants,
            "prepared_participants": self.prepared_participants,
            "fencing_tokens": self.fencing_tokens,
            "timeout_seconds": self.timeout_seconds,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TxRecord":
        raw_ops = data.get("operations", [])
        parsed_ops = [TxOperation.from_dict(o) if isinstance(o, dict) else o for o in raw_ops]
        return cls(
            tx_id=data["tx_id"],
            coordinator_id=data["coordinator_id"],
            status=TxStatus(data.get("status", TxStatus.ACTIVE.value)),
            isolation_level=TxIsolationLevel(data.get("isolation_level", TxIsolationLevel.SERIALIZABLE.value)),
            operations=parsed_ops,
            read_set=data.get("read_set", {}),
            write_set=data.get("write_set", {}),
            participants=data.get("participants", []),
            prepared_participants=data.get("prepared_participants", []),
            fencing_tokens=data.get("fencing_tokens", {}),
            timeout_seconds=float(data.get("timeout_seconds", 10.0)),
            created_at=float(data.get("created_at", time.time())),
            completed_at=float(data["completed_at"]) if data.get("completed_at") is not None else None,
            error_message=data.get("error_message"),
        )


class BarrierState(str, Enum):
    """Lifecycle states for distributed barriers and rendezvous points."""
    WAITING = "WAITING"        # Awaiting required number of parties to enter
    RELEASED = "RELEASED"      # Quorum threshold met, all waiting parties released
    TIMED_OUT = "TIMED_OUT"    # Timeout elapsed before threshold reached
    CANCELLED = "CANCELLED"    # Manually aborted/cancelled by coordinator


@dataclass
class DistributedBarrierSpec:
    """
    Consensus-backed distributed synchronization barrier specification.
    Coordinates N distinct nodes across parallel compute stages.
    """
    barrier_id: str
    threshold: int
    parties: List[str] = field(default_factory=list)
    state: BarrierState = BarrierState.WAITING
    timeout_seconds: float = 30.0
    generation: int = 0
    created_at: float = field(default_factory=time.time)
    released_at: Optional[float] = None

    def is_expired(self, now: Optional[float] = None) -> bool:
        if self.state != BarrierState.WAITING:
            return False
        current_ts = now if now is not None else time.time()
        return current_ts > (self.created_at + self.timeout_seconds)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "barrier_id": self.barrier_id,
            "threshold": self.threshold,
            "parties": self.parties,
            "state": self.state.value if isinstance(self.state, BarrierState) else str(self.state),
            "timeout_seconds": self.timeout_seconds,
            "generation": self.generation,
            "created_at": self.created_at,
            "released_at": self.released_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DistributedBarrierSpec":
        return cls(
            barrier_id=data["barrier_id"],
            threshold=int(data["threshold"]),
            parties=data.get("parties", []),
            state=BarrierState(data.get("state", BarrierState.WAITING.value)),
            timeout_seconds=float(data.get("timeout_seconds", 30.0)),
            generation=int(data.get("generation", 0)),
            created_at=float(data.get("created_at", time.time())),
            released_at=float(data["released_at"]) if data.get("released_at") is not None else None,
        )


@dataclass
class DistributedSemaphoreSpec:
    """
    Consensus-backed distributed counting semaphore specification with lease TTL.
    """
    semaphore_id: str
    total_permits: int
    available_permits: int
    holders: Dict[str, float] = field(default_factory=dict)  # holder_id -> lease_expiry
    default_ttl_seconds: float = 30.0

    def cleanup_expired_leases(self, now: Optional[float] = None) -> int:
        """Reclaim permits whose lease TTL has elapsed."""
        current_ts = now if now is not None else time.time()
        expired = [h for h, exp in self.holders.items() if exp <= current_ts]
        for h in expired:
            del self.holders[h]
            self.available_permits = min(self.total_permits, self.available_permits + 1)
        return len(expired)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "semaphore_id": self.semaphore_id,
            "total_permits": self.total_permits,
            "available_permits": self.available_permits,
            "holders": self.holders,
            "default_ttl_seconds": self.default_ttl_seconds,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DistributedSemaphoreSpec":
        return cls(
            semaphore_id=data["semaphore_id"],
            total_permits=int(data["total_permits"]),
            available_permits=int(data["available_permits"]),
            holders=data.get("holders", {}),
            default_ttl_seconds=float(data.get("default_ttl_seconds", 30.0)),
        )


class BackpressureStatus(str, Enum):
    """Cluster load and backpressure urgency states."""
    NORMAL = "NORMAL"        # Composite load < 0.60, 100% throughput admitted
    MODERATE = "MODERATE"    # Composite load 0.60 - 0.75, slight token rate throttling
    HIGH = "HIGH"            # Composite load 0.75 - 0.85, aggressive rate throttling, BACKGROUND shed
    CRITICAL = "CRITICAL"    # Composite load > 0.85, severe backpressure, only CRITICAL admitted


@dataclass
class TokenBucketConfig:
    """Token Bucket rate limiter configuration."""
    capacity: float = 100.0
    refill_rate: float = 50.0            # Tokens replenished per second
    min_refill_rate: float = 5.0         # Floor under maximum backpressure
    backpressure_threshold: float = 0.85 # Load watermark triggering load shedding

    def to_dict(self) -> Dict[str, Any]:
        return {
            "capacity": self.capacity,
            "refill_rate": self.refill_rate,
            "min_refill_rate": self.min_refill_rate,
            "backpressure_threshold": self.backpressure_threshold,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TokenBucketConfig":
        return cls(
            capacity=float(data.get("capacity", 100.0)),
            refill_rate=float(data.get("refill_rate", 50.0)),
            min_refill_rate=float(data.get("min_refill_rate", 5.0)),
            backpressure_threshold=float(data.get("backpressure_threshold", 0.85)),
        )


@dataclass
class LoadShedderMetrics:
    """Real-time operational telemetry from the adaptive load-shedding engine."""
    cpu_percent: float = 0.0
    ram_percent: float = 0.0
    composite_watermark: float = 0.0
    status: BackpressureStatus = BackpressureStatus.NORMAL
    total_admitted: int = 0
    total_shed: int = 0
    effective_rate: float = 50.0
    current_concurrency: int = 0
    max_concurrency: int = 16

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cpu_percent": self.cpu_percent,
            "ram_percent": self.ram_percent,
            "composite_watermark": self.composite_watermark,
            "status": self.status.value if isinstance(self.status, BackpressureStatus) else str(self.status),
            "total_admitted": self.total_admitted,
            "total_shed": self.total_shed,
            "effective_rate": self.effective_rate,
            "current_concurrency": self.current_concurrency,
            "max_concurrency": self.max_concurrency,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LoadShedderMetrics":
        return cls(
            cpu_percent=float(data.get("cpu_percent", 0.0)),
            ram_percent=float(data.get("ram_percent", 0.0)),
            composite_watermark=float(data.get("composite_watermark", 0.0)),
            status=BackpressureStatus(data.get("status", BackpressureStatus.NORMAL.value)),
            total_admitted=int(data.get("total_admitted", 0)),
            total_shed=int(data.get("total_shed", 0)),
            effective_rate=float(data.get("effective_rate", 50.0)),
            current_concurrency=int(data.get("current_concurrency", 0)),
            max_concurrency=int(data.get("max_concurrency", 16)),
        )

