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
