import asyncio
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
    ConsensusJob,
    ConsensusJobStatus,
    DistributedLock,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    LockAcquireResult,
    LogEntry,
    Message,
    MessageType,
    RaftCommandType,
    WALRecordType,
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
    Compare-And-Swap (CAS), monotonic fencing distributed locks, batch transactions,
    and consensus-backed distributed job queue execution.
    """

    def __init__(self) -> None:
        self._state: Dict[str, Any] = {}
        self._locks: Dict[str, DistributedLock] = {}
        self._jobs: Dict[str, ConsensusJob] = {}
        self._cluster_members: Set[str] = set()
        self._fencing_token_counter: int = 0
        self._commands_applied: int = 0

    @property
    def commands_applied(self) -> int:
        return self._commands_applied

    @property
    def key_count(self) -> int:
        return len(self._state)

    @property
    def job_count(self) -> int:
        return len(self._jobs)

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

    def get_job(self, job_id: str) -> Optional[ConsensusJob]:
        """Query state of a consensus-replicated job."""
        return self._jobs.get(job_id)

    def list_jobs(self, status: Optional[ConsensusJobStatus] = None) -> List[ConsensusJob]:
        """List all consensus jobs optionally filtered by status."""
        if status is None:
            return list(self._jobs.values())
        return [j for j in self._jobs.values() if j.status == status]

    def get_cluster_members(self) -> List[str]:
        """Return list of registered consensus cluster members."""
        return sorted(list(self._cluster_members))

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

        elif cmd == RaftCommandType.JOB_SUBMIT:
            return self._apply_job_submit(entry)

        elif cmd == RaftCommandType.JOB_ASSIGN:
            return self._apply_job_assign(entry)

        elif cmd == RaftCommandType.JOB_COMPLETE:
            return self._apply_job_complete(entry)

        elif cmd == RaftCommandType.JOB_FAIL:
            return self._apply_job_fail(entry)

        elif cmd == RaftCommandType.JOB_CANCEL:
            return self._apply_job_cancel(entry)

        elif cmd == RaftCommandType.MEMBERSHIP_CHANGE:
            return self._apply_membership_change(entry)

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

    def _apply_job_submit(self, entry: LogEntry) -> Dict[str, Any]:
        """Submit new job into consensus replicated queue."""
        job_id = entry.key or entry.extra_data.get("job_id", "")
        job = ConsensusJob(
            job_id=job_id,
            func_bytes=entry.extra_data.get("func_bytes"),
            args_bytes=entry.extra_data.get("args_bytes"),
            kwargs_bytes=entry.extra_data.get("kwargs_bytes"),
            priority=int(entry.extra_data.get("priority", 2)),
            status=ConsensusJobStatus.SUBMITTED,
            submitted_by=entry.client_id or "",
            timeout_seconds=float(entry.extra_data.get("timeout_seconds", 60.0)),
            max_retries=int(entry.extra_data.get("max_retries", 3)),
            created_at=entry.timestamp or time.time(),
        )
        self._jobs[job_id] = job
        return job.to_dict()

    def _apply_job_assign(self, entry: LogEntry) -> Optional[Dict[str, Any]]:
        """Assign job to designated worker node with monotonic fencing token."""
        job_id = entry.key or entry.extra_data.get("job_id", "")
        worker_id = entry.extra_data.get("worker_id", "")
        if job_id in self._jobs:
            job = self._jobs[job_id]
            self._fencing_token_counter += 1
            job.status = ConsensusJobStatus.ASSIGNED
            job.assigned_to = worker_id
            job.assigned_at = entry.timestamp or time.time()
            job.fencing_token = self._fencing_token_counter
            return job.to_dict()
        return None

    def _apply_job_complete(self, entry: LogEntry) -> bool:
        """Mark job completed with result and timestamp."""
        job_id = entry.key or entry.extra_data.get("job_id", "")
        fencing_token = entry.fencing_token or entry.extra_data.get("fencing_token")
        result_bytes = entry.extra_data.get("result_bytes")

        if job_id in self._jobs:
            job = self._jobs[job_id]
            if fencing_token is not None and job.fencing_token != fencing_token:
                return False
            job.status = ConsensusJobStatus.COMPLETED
            job.result_bytes = result_bytes
            job.completed_at = entry.timestamp or time.time()
            return True
        return False

    def _apply_job_fail(self, entry: LogEntry) -> Optional[Dict[str, Any]]:
        """Handle job execution failure with retry re-queueing."""
        job_id = entry.key or entry.extra_data.get("job_id", "")
        error_msg = entry.extra_data.get("error_message")

        if job_id in self._jobs:
            job = self._jobs[job_id]
            job.retry_count += 1
            job.error_message = error_msg
            if job.retry_count >= job.max_retries:
                job.status = ConsensusJobStatus.FAILED
                job.completed_at = entry.timestamp or time.time()
            else:
                # Re-queue for re-assignment
                job.status = ConsensusJobStatus.SUBMITTED
                job.assigned_to = None
                job.assigned_at = None
            return job.to_dict()
        return None

    def _apply_job_cancel(self, entry: LogEntry) -> bool:
        """Cancel submitted or running job."""
        job_id = entry.key or entry.extra_data.get("job_id", "")
        if job_id in self._jobs:
            job = self._jobs[job_id]
            if job.status not in (ConsensusJobStatus.COMPLETED, ConsensusJobStatus.CANCELLED):
                job.status = ConsensusJobStatus.CANCELLED
                job.completed_at = entry.timestamp or time.time()
                return True
        return False

    def _apply_membership_change(self, entry: LogEntry) -> List[str]:
        """Dynamically add or remove cluster consensus members."""
        action = entry.extra_data.get("action", "ADD")
        peer = entry.key
        if peer:
            if action == "ADD":
                self._cluster_members.add(peer)
            elif action == "REMOVE":
                self._cluster_members.discard(peer)
        return sorted(list(self._cluster_members))

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
        """Export full snapshot representation of KV state, active locks, jobs, and membership."""
        now = time.time()
        return {
            "state": dict(self._state),
            "locks": {k: v.to_dict() for k, v in self._locks.items() if not v.is_expired(now)},
            "jobs": {k: v.to_dict() for k, v in self._jobs.items()},
            "cluster_members": list(self._cluster_members),
            "fencing_token_counter": self._fencing_token_counter,
            "commands_applied": self._commands_applied,
        }

    def import_state(self, snapshot_data: Dict[str, Any]) -> None:
        """Restore state machine from snapshot representation."""
        self._state = dict(snapshot_data.get("state", {}))
        self._fencing_token_counter = int(snapshot_data.get("fencing_token_counter", 0))
        self._commands_applied = int(snapshot_data.get("commands_applied", 0))
        self._cluster_members = set(snapshot_data.get("cluster_members", []))
        self._locks = {}
        for k, v in snapshot_data.get("locks", {}).items():
            self._locks[k] = DistributedLock.from_dict(v)
        self._jobs = {}
        for k, v in snapshot_data.get("jobs", {}).items():
            self._jobs[k] = ConsensusJob.from_dict(v)




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
    total_snapshots_sent: int = 0
    total_snapshots_installed: int = 0
    jobs_count: int = 0

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
            "total_snapshots_sent": self.total_snapshots_sent,
            "total_snapshots_installed": self.total_snapshots_installed,
            "jobs_count": self.jobs_count,
        }


class RaftReplicationEngine:
    """
    Raft Log Replication Engine managing proposal submission, AppendEntries RPCs,
    follower catch-up, snapshot streaming (InstallSnapshot RPC), majority quorum
    commit consensus, and state machine application.
    """

    def __init__(
        self,
        node_id: str,
        log: Optional[RaftLog] = None,
        state_machine: Optional[ReplicatedStateMachine] = None,
        get_active_peers_fn: Optional[Callable[[], List[Tuple[str, int]]]] = None,
        send_message_fn: Optional[Callable[[str, int, Message], None]] = None,
        get_term_and_role_fn: Optional[Callable[[], Tuple[int, str]]] = None,
        wal_engine: Optional[Any] = None,
    ):
        self.node_id = node_id
        self.log = log or RaftLog()
        self.state_machine = state_machine or ReplicatedStateMachine()
        self.get_active_peers = get_active_peers_fn or (lambda: [])
        self.send_message = send_message_fn or (lambda host, port, msg: None)
        self.get_term_and_role = get_term_and_role_fn or (lambda: (1, "LEADER"))
        self.wal_engine = wal_engine

        self.followers: Dict[str, FollowerProgress] = {}
        self._pending_proposals: Dict[int, Any] = {}  # index -> asyncio.Future

        # Metrics
        self.total_proposals = 0
        self.total_committed = 0
        self.total_replications_sent = 0
        self.replication_successes = 0
        self.replication_failures = 0
        self.total_snapshots_sent = 0
        self.total_snapshots_installed = 0

    def initialize_follower(self, follower_id: str) -> None:
        """Initialize progress tracking for a new or re-connected follower."""
        if follower_id not in self.followers:
            self.followers[follower_id] = FollowerProgress(
                node_id=follower_id,
                match_index=0,
                next_index=self.log.last_index + 1,
            )

    def create_snapshot(self, last_included_index: Optional[int] = None) -> Dict[str, Any]:
        """
        Create snapshot of state machine and compact log up to target index.
        """
        target_idx = last_included_index if last_included_index is not None else self.log.last_applied
        if target_idx <= 0:
            return {"snapshot_last_index": 0, "snapshot_last_term": 0, "data": self.state_machine.export_state()}

        self.log.compact_log_before(target_idx)
        return {
            "snapshot_last_index": self.log._snapshot_last_index,
            "snapshot_last_term": self.log._snapshot_last_term,
            "data": self.state_machine.export_state(),
        }

    def broadcast_append_entries(self) -> None:
        """Dispatch AppendEntries or InstallSnapshot RPC requests to all active cluster peers."""
        term, role = self.get_term_and_role()
        if role != "LEADER":
            return

        active_peers = self.get_active_peers()
        for host, port in active_peers:
            peer_id = f"{host}:{port}"
            follower = self.followers.get(peer_id)
            next_idx = follower.next_index if follower else 1

            # Check if follower has fallen behind the compacted log boundary
            if self.log._snapshot_last_index > 0 and next_idx <= self.log._snapshot_last_index:
                # Follower needs an InstallSnapshot RPC
                snapshot_req = InstallSnapshotRequest(
                    term=term,
                    leader_id=self.node_id,
                    last_included_index=self.log._snapshot_last_index,
                    last_included_term=self.log._snapshot_last_term,
                    data=self.state_machine.export_state(),
                    done=True,
                )
                msg = Message(
                    type=MessageType.RAFT_INSTALL_SNAPSHOT_REQUEST,
                    sender_id=self.node_id,
                    sender_udp_port=0,
                    payload=snapshot_req.to_dict(),
                )
                try:
                    self.send_message(host, port, msg)
                    self.total_snapshots_sent += 1
                except Exception as e:
                    logger.debug(f"Failed to send InstallSnapshot to {host}:{port}: {e}")
                continue

            prev_idx = next_idx - 1
            prev_term = self.log.get_term(prev_idx)
            entries_to_send = self.log.slice_from(next_idx)

            req = AppendEntriesRequest(
                term=term,
                leader_id=self.node_id,
                prev_log_index=prev_idx,
                prev_log_term=prev_term,
                entries=entries_to_send,
                leader_commit=self.log.commit_index,
            )
            msg = Message(
                type=MessageType.RAFT_APPEND_ENTRIES_REQUEST,
                sender_id=self.node_id,
                sender_udp_port=0,
                payload=req.to_dict(),
            )
            try:
                self.send_message(host, port, msg)
                self.total_replications_sent += 1
            except Exception as e:
                logger.debug(f"Failed to send AppendEntries to {host}:{port}: {e}")

    def handle_append_entries_request(self, msg: Message, addr: Tuple[str, int]) -> Optional[Message]:
        """
        Follower receiver implementation for Raft AppendEntries RPC:
        Validates leader term, verifies log consistency, reconciles entries,
        and advances local commit_index.
        """
        try:
            req = AppendEntriesRequest.from_dict(msg.payload)
        except Exception as e:
            logger.warning(f"Malformed AppendEntriesRequest: {e}")
            return None

        current_term, _ = self.get_term_and_role()

        # Rule 1: Reply False if term < current_term
        if req.term < current_term:
            resp = AppendEntriesResponse(
                term=current_term,
                follower_id=self.node_id,
                success=False,
                match_index=self.log.last_index,
                last_log_index=self.log.last_index,
                error_message="Stale leader term",
            )
            return Message(
                msg_id=msg.msg_id,
                type=MessageType.RAFT_APPEND_ENTRIES_RESPONSE,
                sender_id=self.node_id,
                sender_udp_port=0,
                payload=resp.to_dict(),
            )

        # Rule 2: Reconcile entries against log matching invariant
        success, match_idx = self.log.reconcile_follower_entries(
            prev_log_index=req.prev_log_index,
            prev_log_term=req.prev_log_term,
            new_entries=req.entries,
        )

        if success:
            # Rule 3: Advance follower commit index if leader_commit > commit_index
            if req.leader_commit > self.log.commit_index:
                new_commit = min(req.leader_commit, match_idx)
                self.log.advance_commit_index(new_commit)
                self.apply_committed_entries()

        resp = AppendEntriesResponse(
            term=max(current_term, req.term),
            follower_id=self.node_id,
            success=success,
            match_index=match_idx if success else self.log.last_index,
            last_log_index=self.log.last_index,
        )
        return Message(
            msg_id=msg.msg_id,
            type=MessageType.RAFT_APPEND_ENTRIES_RESPONSE,
            sender_id=self.node_id,
            sender_udp_port=0,
            payload=resp.to_dict(),
        )

    def handle_install_snapshot_request(self, msg: Message, addr: Tuple[str, int]) -> Optional[Message]:
        """
        Follower receiver implementation for Raft InstallSnapshot RPC:
        Validates term, installs state machine snapshot, compacts local log,
        and advances commit/applied indices.
        """
        try:
            req = InstallSnapshotRequest.from_dict(msg.payload)
        except Exception as e:
            logger.warning(f"Malformed InstallSnapshotRequest: {e}")
            return None

        current_term, _ = self.get_term_and_role()

        if req.term < current_term:
            resp = InstallSnapshotResponse(
                term=current_term,
                follower_id=self.node_id,
                success=False,
                match_index=self.log.last_index,
                error_message="Stale leader term",
            )
            return Message(
                msg_id=msg.msg_id,
                type=MessageType.RAFT_INSTALL_SNAPSHOT_RESPONSE,
                sender_id=self.node_id,
                sender_udp_port=0,
                payload=resp.to_dict(),
            )

        # Restore state machine from snapshot
        self.state_machine.import_state(req.data)
        self.log.restore_snapshot({
            "snapshot_last_index": req.last_included_index,
            "snapshot_last_term": req.last_included_term,
        })
        self.total_snapshots_installed += 1

        resp = InstallSnapshotResponse(
            term=max(current_term, req.term),
            follower_id=self.node_id,
            success=True,
            match_index=req.last_included_index,
        )
        return Message(
            msg_id=msg.msg_id,
            type=MessageType.RAFT_INSTALL_SNAPSHOT_RESPONSE,
            sender_id=self.node_id,
            sender_udp_port=0,
            payload=resp.to_dict(),
        )

    def synchronize_follower_progress(self, follower_id: str, success: bool, match_index: int, last_index: int) -> None:
        """Update follower progress pointers based on RPC outcome."""
        self.initialize_follower(follower_id)
        follower = self.followers[follower_id]
        follower.last_ack_time = time.time()

        if success:
            follower.match_index = max(follower.match_index, match_index)
            follower.next_index = follower.match_index + 1
            self.replication_successes += 1
        else:
            # Step down next_index on conflict to find common ancestor
            follower.next_index = max(1, min(follower.next_index - 1, last_index + 1))
            self.replication_failures += 1

    def check_and_advance_quorum_commit(self) -> int:
        """
        Check if there exists an N > commit_index such that a majority of
        match_index[i] >= N and log[N].term == current_term.
        Advances commit_index if quorum is satisfied.
        """
        term, role = self.get_term_and_role()
        if role != "LEADER":
            return self.log.commit_index

        active_peers = self.get_active_peers()
        total_cluster = len(active_peers) + 1
        quorum_required = (total_cluster // 2) + 1

        # Collect match indices without duplicates across aliases (node_id vs host:port)
        match_indices = [self.log.last_index]
        seen_keys = set()
        for host, port in active_peers:
            peer_key = f"{host}:{port}"
            if peer_key in self.followers:
                match_indices.append(self.followers[peer_key].match_index)
                seen_keys.add(peer_key)

        for fid, f in self.followers.items():
            if fid not in seen_keys and ":" not in fid:
                match_indices.append(f.match_index)
                seen_keys.add(fid)

        match_indices.sort(reverse=True)

        for candidate_idx in range(self.log.last_index, self.log.commit_index, -1):
            if self.log.get_term(candidate_idx) == term:
                count = sum(1 for m in match_indices if m >= candidate_idx)
                if count >= quorum_required:
                    self.log.advance_commit_index(candidate_idx)
                    self.total_committed += 1
                    self.apply_committed_entries()
                    break

        return self.log.commit_index

    def handle_append_entries_response(self, msg: Message, addr: Optional[Tuple[str, int]] = None) -> None:
        """Process follower AppendEntriesResponse on leader."""
        try:
            resp = AppendEntriesResponse.from_dict(msg.payload)
        except Exception as e:
            logger.warning(f"Malformed AppendEntriesResponse: {e}")
            return

        follower_ids = set()
        if resp.follower_id:
            follower_ids.add(resp.follower_id)
        if msg.sender_id:
            follower_ids.add(msg.sender_id)
        if addr:
            follower_ids.add(f"{addr[0]}:{addr[1]}")
        elif msg.sender_udp_port:
            follower_ids.add(f"127.0.0.1:{msg.sender_udp_port}")

        for fid in follower_ids:
            self.synchronize_follower_progress(
                follower_id=fid,
                success=resp.success,
                match_index=resp.match_index,
                last_index=resp.last_log_index,
            )

        if resp.success:
            self.check_and_advance_quorum_commit()

    def handle_install_snapshot_response(self, msg: Message, addr: Optional[Tuple[str, int]] = None) -> None:
        """Process follower InstallSnapshotResponse on leader."""
        try:
            resp = InstallSnapshotResponse.from_dict(msg.payload)
        except Exception as e:
            logger.warning(f"Malformed InstallSnapshotResponse: {e}")
            return

        follower_ids = set()
        if resp.follower_id:
            follower_ids.add(resp.follower_id)
        if msg.sender_id:
            follower_ids.add(msg.sender_id)
        if addr:
            follower_ids.add(f"{addr[0]}:{addr[1]}")
        elif msg.sender_udp_port:
            follower_ids.add(f"127.0.0.1:{msg.sender_udp_port}")

        for fid in follower_ids:
            self.initialize_follower(fid)
            follower = self.followers[fid]
            follower.last_ack_time = time.time()
            if resp.success:
                follower.match_index = max(follower.match_index, resp.match_index)
                follower.next_index = follower.match_index + 1

        if resp.success:
            self.replication_successes += 1
            self.check_and_advance_quorum_commit()
            if any(f.next_index <= self.log.last_index for f in self.followers.values()):
                self.broadcast_append_entries()
        else:
            self.replication_failures += 1


    def apply_committed_entries(self) -> List[Tuple[int, Any]]:
        """Apply all newly committed entries to the state machine in strict sequential order."""
        unapplied = self.log.get_unapplied_entries()
        results: List[Tuple[int, Any]] = []

        for entry in unapplied:
            res = self.state_machine.apply_command(entry)
            self.log.mark_applied(entry.index)
            results.append((entry.index, res))

            # Resolve pending client proposal future if registered
            if entry.index in self._pending_proposals:
                fut = self._pending_proposals.pop(entry.index)
                if not fut.done():
                    fut.set_result(res)

        return results

    async def propose_command(
        self,
        command_type: RaftCommandType,
        key: Optional[str] = None,
        value: Optional[Any] = None,
        client_id: Optional[str] = None,
        fencing_token: Optional[int] = None,
        extra_data: Optional[Dict[str, Any]] = None,
        timeout: float = 5.0,
    ) -> Any:
        """
        Submit a new state command through the Raft replication engine:
        Appends to leader log -> dispatches AppendEntries -> waits for quorum commit -> returns result.
        """
        term, role = self.get_term_and_role()
        if role != "LEADER":
            raise RuntimeError(f"Cannot propose command on non-leader node (current role: {role})")

        self.total_proposals += 1
        entry = self.log.append_command(
            term=term,
            command_type=command_type,
            key=key,
            value=value,
            client_id=client_id or self.node_id,
            fencing_token=fencing_token,
            extra_data=extra_data or {},
        )

        active_peers = self.get_active_peers()
        # Single node cluster fast-path
        if not active_peers:
            self.log.advance_commit_index(entry.index)
            results = self.apply_committed_entries()
            return results[-1][1] if results else None

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending_proposals[entry.index] = future

        self.broadcast_append_entries()

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_proposals.pop(entry.index, None)
            raise TimeoutError(f"Proposal for index {entry.index} timed out waiting for consensus quorum after {timeout}s")

    def get_raft_metrics(self) -> RaftMetrics:
        """Capture live Raft log replication and state machine metrics."""
        term, role = self.get_term_and_role()
        return RaftMetrics(
            node_id=self.node_id,
            role=role,
            current_term=term,
            last_log_index=self.log.last_index,
            last_log_term=self.log.last_term,
            commit_index=self.log.commit_index,
            last_applied=self.log.last_applied,
            total_proposals=self.total_proposals,
            total_committed=self.total_committed,
            total_replications_sent=self.total_replications_sent,
            replication_successes=self.replication_successes,
            replication_failures=self.replication_failures,
            active_locks_count=self.state_machine.active_lock_count,
            state_keys_count=self.state_machine.key_count,
            total_snapshots_sent=self.total_snapshots_sent,
            total_snapshots_installed=self.total_snapshots_installed,
            jobs_count=self.state_machine.job_count,
        )

