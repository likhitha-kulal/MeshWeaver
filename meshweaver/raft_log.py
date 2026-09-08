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
