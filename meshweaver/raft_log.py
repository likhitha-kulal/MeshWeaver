"""
MeshWeaver Replicated State Machine & Raft Log Replication Engine
Implements in-memory append-only Raft commit log, log matching invariants,
deterministic state machine execution, distributed locking with fencing tokens,
majority quorum commit calculations, and follower log synchronization.
"""

from dataclasses import dataclass, field
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from meshweaver.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    DistributedLock,
    LockAcquireResult,
    LogEntry,
    Message,
    MessageType,
    RaftCommandType,
)

logger = logging.getLogger("meshweaver.raft")


class RaftLog:
    """
    In-memory term-indexed Raft log storage with 1-based index numbering.
    Maintains commit_index, applied_index, and compaction offsets.
    """

    def __init__(self) -> None:
        self._entries: List[LogEntry] = []
        self._commit_index: int = 0
        self._last_applied: int = 0
        self._snapshot_last_index: int = 0
        self._snapshot_last_term: int = 0

    @property
    def commit_index(self) -> int:
        return self._commit_index

    @property
    def last_applied(self) -> int:
        return self._last_applied

    @property
    def count(self) -> int:
        return len(self._entries)

    @property
    def last_index(self) -> int:
        if not self._entries:
            return self._snapshot_last_index
        return self._entries[-1].index

    @property
    def last_term(self) -> int:
        if not self._entries:
            return self._snapshot_last_term
        return self._entries[-1].term

    def append_entry(self, entry: LogEntry) -> int:
        """Append a new log entry, ensuring sequential 1-based indexing."""
        expected_index = self.last_index + 1
        if entry.index <= 0:
            entry.index = expected_index
        elif entry.index != expected_index:
            raise ValueError(f"Log entry index gap: expected {expected_index}, got {entry.index}")

        self._entries.append(entry)
        return entry.index

    def append_command(
        self,
        term: int,
        command_type: RaftCommandType,
        key: Optional[str] = None,
        value: Optional[Any] = None,
        client_id: Optional[str] = None,
        fencing_token: Optional[int] = None,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> LogEntry:
        """Create and append a LogEntry for a command."""
        entry = LogEntry(
            index=self.last_index + 1,
            term=term,
            command_type=command_type,
            key=key,
            value=value,
            client_id=client_id,
            timestamp=time.time(),
            fencing_token=fencing_token,
            extra_data=extra_data or {},
        )
        self.append_entry(entry)
        return entry

    def get_entry(self, index: int) -> Optional[LogEntry]:
        """Retrieve entry at 1-based index."""
        if index <= self._snapshot_last_index:
            return None
        offset = index - self._snapshot_last_index - 1
        if 0 <= offset < len(self._entries):
            return self._entries[offset]
        return None

    def get_term(self, index: int) -> int:
        """Return term for entry at given index. Returns 0 if index is 0 or uncommitted base."""
        if index == 0:
            return 0
        if index == self._snapshot_last_index:
            return self._snapshot_last_term
        entry = self.get_entry(index)
        return entry.term if entry is not None else 0

    def slice_from(self, start_index: int) -> List[LogEntry]:
        """Return list of entries starting from start_index up to the end of the log."""
        if start_index > self.last_index:
            return []
        offset = max(0, start_index - self._snapshot_last_index - 1)
        return list(self._entries[offset:])

    def check_consistency(self, prev_log_index: int, prev_log_term: int) -> bool:
        """Check if local log contains an entry at prev_log_index with matching prev_log_term."""
        if prev_log_index == 0:
            return True
        if prev_log_index == self._snapshot_last_index:
            return self._snapshot_last_term == prev_log_term
        entry = self.get_entry(prev_log_index)
        if entry is None:
            return False
        return entry.term == prev_log_term

    def truncate_from(self, from_index: int) -> int:
        """
        Truncate all log entries starting from from_index (inclusive).
        Used during conflict resolution when follower diverges from leader.
        """
        if from_index <= self._snapshot_last_index:
            raise ValueError(f"Cannot truncate compacted snapshot log below {self._snapshot_last_index}")
        if from_index > self.last_index:
            return 0

        offset = from_index - self._snapshot_last_index - 1
        removed_count = len(self._entries) - offset
        self._entries = self._entries[:offset]
        if self._commit_index > self.last_index:
            self._commit_index = self.last_index
        return removed_count

    def reconcile_follower_entries(
        self,
        prev_log_index: int,
        prev_log_term: int,
        new_entries: List[LogEntry],
    ) -> Tuple[bool, int]:
        """
        Implements Raft AppendEntries log consistency and conflict resolution:
        1. Reply False if log doesn't contain an entry at prev_log_index matching prev_log_term.
        2. If an existing entry conflicts with a new one (same index but different terms),
           delete existing entry and all that follow it.
        3. Append any new entries not already in the log.
        Returns: (success: bool, match_index: int)
        """
        if not self.check_consistency(prev_log_index, prev_log_term):
            return False, self.last_index

        for entry in new_entries:
            existing = self.get_entry(entry.index)
            if existing is not None:
                if existing.term != entry.term:
                    # Conflict found -> truncate conflicting entries
                    self.truncate_from(entry.index)
                    self.append_entry(entry)
            else:
                self.append_entry(entry)

        return True, self.last_index

    def advance_commit_index(self, new_commit_index: int) -> int:
        """
        Safely advance the commit index up to min(new_commit_index, last_index).
        Cannot move backwards.
        """
        valid_commit = min(new_commit_index, self.last_index)
        if valid_commit > self._commit_index:
            self._commit_index = valid_commit
        return self._commit_index

    def get_unapplied_entries(self) -> List[LogEntry]:
        """Return all committed entries that have not yet been applied to the state machine."""
        if self._commit_index <= self._last_applied:
            return []
        start_idx = self._last_applied + 1
        unapplied: List[LogEntry] = []
        for idx in range(start_idx, self._commit_index + 1):
            entry = self.get_entry(idx)
            if entry is not None:
                unapplied.append(entry)
        return unapplied

    def mark_applied(self, index: int) -> None:
        """Update last_applied index to acknowledge state machine execution."""
        if index > self._last_applied:
            self._last_applied = min(index, self._commit_index)

    def create_snapshot(self) -> Dict[str, Any]:
        """Create snapshot metadata of current log compaction state."""
        return {
            "snapshot_last_index": self._snapshot_last_index,
            "snapshot_last_term": self._snapshot_last_term,
            "commit_index": self._commit_index,
            "last_applied": self._last_applied,
        }

    def compact_log_before(self, index: int) -> int:
        """
        Trim log entries before index (which must be <= last_applied).
        Replaces trimmed portion with snapshot markers to bound memory usage.
        """
        if index <= self._snapshot_last_index:
            return 0
        target = min(index, self._last_applied)
        entry = self.get_entry(target)
        if entry is None:
            return 0

        self._snapshot_last_index = entry.index
        self._snapshot_last_term = entry.term
        offset = target - (self._snapshot_last_index - len(self._entries))
        trimmed_count = target - self._snapshot_last_index
        self._entries = [e for e in self._entries if e.index > target]
        return len(self._entries)

    def restore_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Restore compaction markers from snapshot."""
        self._snapshot_last_index = int(snapshot.get("snapshot_last_index", 0))
        self._snapshot_last_term = int(snapshot.get("snapshot_last_term", 0))
        self._commit_index = max(self._commit_index, self._snapshot_last_index)
        self._last_applied = max(self._last_applied, self._snapshot_last_index)
        self._entries = [e for e in self._entries if e.index > self._snapshot_last_index]



class ReplicatedStateMachine:
    """
    Deterministic replicated state machine supporting atomic KV operations,
    Compare-And-Swap (CAS), monotonic fencing distributed locks, and batch transactions.
    """

    def __init__(self) -> None:
        self._state: Dict[str, Any] = {}
        self._locks: Dict[str, DistributedLock] = {}
        self._fencing_token_counter: int = 0
        self._commands_applied: int = 0

    @property
    def commands_applied(self) -> int:
        return self._commands_applied

    @property
    def key_count(self) -> int:
        return len(self._state)

    @property
    def active_lock_count(self) -> int:
        now = time.time()
        return sum(1 for l in self._locks.values() if not l.is_expired(now))

    def get(self, key: str, default: Any = None) -> Any:
        """Local non-mutating query on current state machine view."""
        return self._state.get(key, default)

    def contains(self, key: str) -> bool:
        return key in self._state

    def get_all_keys(self) -> List[str]:
        return list(self._state.keys())

    def apply_command(self, entry: LogEntry) -> Any:
        """
        Deterministically apply a committed LogEntry to the state machine.
        Returns the command execution result.
        """
        self._commands_applied += 1
        cmd = entry.command_type

        if cmd == RaftCommandType.SET:
            if entry.key is not None:
                self._state[entry.key] = entry.value
            return entry.value

        elif cmd == RaftCommandType.GET:
            return self._state.get(entry.key) if entry.key else None

        elif cmd == RaftCommandType.DELETE:
            if entry.key is not None:
                return self._state.pop(entry.key, None)
            return None

        elif cmd == RaftCommandType.CAS:
            # Compare-and-swap
            if entry.key is None:
                return False
            expected = entry.extra_data.get("expected")
            current = self._state.get(entry.key)
            if current == expected:
                self._state[entry.key] = entry.value
                return True
            return False

        elif cmd == RaftCommandType.INCREMENT:
            if entry.key is None:
                return 0
            delta = int(entry.extra_data.get("delta", 1))
            current = int(self._state.get(entry.key, 0))
            new_val = current + delta
            self._state[entry.key] = new_val
            return new_val

        elif cmd == RaftCommandType.LOCK_ACQUIRE:
            return self._apply_lock_acquire(entry)

        elif cmd == RaftCommandType.LOCK_RELEASE:
            return self._apply_lock_release(entry)

        elif cmd == RaftCommandType.BATCH:
            return self._apply_batch(entry)

        elif cmd == RaftCommandType.NOOP:
            return "NOOP_APPLIED"

        return None

    def _apply_lock_acquire(self, entry: LogEntry) -> LockAcquireResult:
        """Process distributed lock acquisition with TTL and monotonic fencing token."""
        resource = entry.key or "default_resource"
        holder_id = entry.client_id or "anonymous"
        ttl_seconds = float(entry.extra_data.get("ttl_seconds", 30.0))
        now = entry.timestamp or time.time()

        existing = self._locks.get(resource)
        if existing is not None and not existing.is_expired(now):
            if existing.holder_id == holder_id:
                # Renew existing lock
                existing.acquired_at = now
                existing.ttl_seconds = ttl_seconds
                return LockAcquireResult(
                    acquired=True,
                    resource=resource,
                    fencing_token=existing.fencing_token,
                    holder_id=holder_id,
                )
            return LockAcquireResult(
                acquired=False,
                resource=resource,
                fencing_token=None,
                holder_id=existing.holder_id,
                error=f"Resource '{resource}' currently locked by {existing.holder_id}",
            )

        self._fencing_token_counter += 1
        new_lock = DistributedLock(
            resource=resource,
            holder_id=holder_id,
            fencing_token=self._fencing_token_counter,
            acquired_at=now,
            ttl_seconds=ttl_seconds,
        )
        self._locks[resource] = new_lock
        return LockAcquireResult(
            acquired=True,
            resource=resource,
            fencing_token=new_lock.fencing_token,
            holder_id=holder_id,
        )

    def _apply_lock_release(self, entry: LogEntry) -> bool:
        """Release distributed lock verifying holder and fencing token."""
        resource = entry.key or "default_resource"
        holder_id = entry.client_id
        fencing_token = entry.fencing_token or entry.extra_data.get("fencing_token")

        existing = self._locks.get(resource)
        if existing is None:
            return True

        if fencing_token is not None and existing.fencing_token != fencing_token:
            return False
        if holder_id is not None and existing.holder_id != holder_id:
            return False

        self._locks.pop(resource, None)
        return True

    def get_lock(self, resource: str) -> Optional[DistributedLock]:
        """Retrieve active lock info for resource if not expired."""
        lock = self._locks.get(resource)
        if lock is not None and not lock.is_expired():
            return lock
        return None

    def cleanup_expired_locks(self) -> int:
        """Evict expired distributed locks."""
        now = time.time()
        expired = [r for r, l in self._locks.items() if l.is_expired(now)]
        for r in expired:
            self._locks.pop(r, None)
        return len(expired)

    def _apply_batch(self, entry: LogEntry) -> List[Any]:
        """Atomically execute a batch of sub-commands."""
        raw_ops = entry.extra_data.get("operations", [])
        results: List[Any] = []
        for op in raw_ops:
            sub_entry = LogEntry.from_dict(op) if isinstance(op, dict) else op
            res = self.apply_command(sub_entry)
            results.append(res)
        return results

    def export_state(self) -> Dict[str, Any]:
        """Export full snapshot representation of KV state and active locks."""
        now = time.time()
        return {
            "state": dict(self._state),
            "locks": {k: v.to_dict() for k, v in self._locks.items() if not v.is_expired(now)},
            "fencing_token_counter": self._fencing_token_counter,
            "commands_applied": self._commands_applied,
        }

    def import_state(self, snapshot_data: Dict[str, Any]) -> None:
        """Restore state machine from snapshot representation."""
        self._state = dict(snapshot_data.get("state", {}))
        self._fencing_token_counter = int(snapshot_data.get("fencing_token_counter", 0))
        self._commands_applied = int(snapshot_data.get("commands_applied", 0))
        self._locks = {}
        for k, v in snapshot_data.get("locks", {}).items():
            self._locks[k] = DistributedLock.from_dict(v)



@dataclass
class FollowerProgress:
    """Tracks replication progress and next log indices for a follower peer."""
    node_id: str
    match_index: int = 0
    next_index: int = 1
    last_ack_time: float = field(default_factory=time.time)


@dataclass
class RaftMetrics:
    """Real-time observability snapshot for Raft log replication and state machine."""
    node_id: str
    role: str
    current_term: int
    last_log_index: int
    last_log_term: int
    commit_index: int
    last_applied: int
    total_proposals: int
    total_committed: int
    total_replications_sent: int
    replication_successes: int
    replication_failures: int
    active_locks_count: int
    state_keys_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "role": self.role,
            "current_term": self.current_term,
            "last_log_index": self.last_log_index,
            "last_log_term": self.last_log_term,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "total_proposals": self.total_proposals,
            "total_committed": self.total_committed,
            "total_replications_sent": self.total_replications_sent,
            "replication_successes": self.replication_successes,
            "replication_failures": self.replication_failures,
            "active_locks_count": self.active_locks_count,
            "state_keys_count": self.state_keys_count,
        }


class RaftReplicationEngine:
    """
    Raft Log Replication Engine managing proposal submission, AppendEntries RPCs,
    follower catch-up, majority quorum commit consensus, and state machine application.
    """

    def __init__(
        self,
        node_id: str,
        log: Optional[RaftLog] = None,
        state_machine: Optional[ReplicatedStateMachine] = None,
        get_active_peers_fn: Optional[Callable[[], List[Tuple[str, int]]]] = None,
        send_message_fn: Optional[Callable[[str, int, Message], None]] = None,
        get_term_and_role_fn: Optional[Callable[[], Tuple[int, str]]] = None,
    ):
        self.node_id = node_id
        self.log = log or RaftLog()
        self.state_machine = state_machine or ReplicatedStateMachine()
        self.get_active_peers = get_active_peers_fn or (lambda: [])
        self.send_message = send_message_fn or (lambda host, port, msg: None)
        self.get_term_and_role = get_term_and_role_fn or (lambda: (1, "LEADER"))

        self.followers: Dict[str, FollowerProgress] = {}
        self._pending_proposals: Dict[int, Any] = {}  # index -> asyncio.Future

        # Metrics
        self.total_proposals = 0
        self.total_committed = 0
        self.total_replications_sent = 0
        self.replication_successes = 0
        self.replication_failures = 0
