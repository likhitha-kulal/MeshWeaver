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
