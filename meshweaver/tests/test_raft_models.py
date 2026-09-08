"""
Unit tests for Raft log replication data models and message serialization.
"""

import time
import pytest
from meshweaver.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    DistributedLock,
    LockAcquireResult,
    LogEntry,
    MessageType,
    RaftCommandType,
)


def test_raft_command_type_enum():
    assert RaftCommandType.SET == "SET"
    assert RaftCommandType.CAS == "CAS"
    assert RaftCommandType.LOCK_ACQUIRE == "LOCK_ACQUIRE"
    assert RaftCommandType.NOOP == "NOOP"


def test_log_entry_serialization():
    entry = LogEntry(
        index=1,
        term=2,
        command_type=RaftCommandType.SET,
        key="cluster_config",
        value={"max_workers": 16, "retries": 3},
        client_id="node_alpha",
        fencing_token=42,
        extra_data={"tag": "v1.0"},
    )
    d = entry.to_dict()
    assert d["index"] == 1
    assert d["term"] == 2
    assert d["command_type"] == "SET"
    assert d["key"] == "cluster_config"
    assert d["fencing_token"] == 42
    assert d["value"]["max_workers"] == 16

    restored = LogEntry.from_dict(d)
    assert restored.index == entry.index
    assert restored.term == entry.term
    assert restored.command_type == RaftCommandType.SET
    assert restored.key == entry.key
    assert restored.value == entry.value
    assert restored.client_id == entry.client_id
    assert restored.fencing_token == 42


def test_append_entries_request_serialization():
    entries = [
        LogEntry(index=1, term=1, command_type=RaftCommandType.SET, key="k1", value="v1"),
        LogEntry(index=2, term=1, command_type=RaftCommandType.SET, key="k2", value="v2"),
    ]
    req = AppendEntriesRequest(
        term=3,
        leader_id="leader_node_1",
        prev_log_index=0,
        prev_log_term=0,
        entries=entries,
        leader_commit=1,
    )
    d = req.to_dict()
    assert d["term"] == 3
    assert d["leader_id"] == "leader_node_1"
    assert len(d["entries"]) == 2
    assert d["leader_commit"] == 1

    restored = AppendEntriesRequest.from_dict(d)
    assert restored.term == 3
    assert restored.leader_id == "leader_node_1"
    assert len(restored.entries) == 2
    assert restored.entries[1].key == "k2"


def test_append_entries_response_serialization():
    resp = AppendEntriesResponse(
        term=3,
        follower_id="follower_node_2",
        success=True,
        match_index=5,
        last_log_index=5,
        error_message=None,
    )
    d = resp.to_dict()
    assert d["success"] is True
    assert d["match_index"] == 5

    restored = AppendEntriesResponse.from_dict(d)
    assert restored.term == 3
    assert restored.follower_id == "follower_node_2"
    assert restored.success is True
    assert restored.match_index == 5


def test_distributed_lock_model():
    now = time.time()
    lock = DistributedLock(
        resource="job_queue_lock",
        holder_id="node_gamma",
        fencing_token=101,
        acquired_at=now,
        ttl_seconds=10.0,
    )
    assert not lock.is_expired(now + 5.0)
    assert lock.is_expired(now + 15.0)
    assert lock.remaining_ttl(now + 4.0) == pytest.approx(6.0, abs=0.1)

    d = lock.to_dict()
    restored = DistributedLock.from_dict(d)
    assert restored.resource == "job_queue_lock"
    assert restored.holder_id == "node_gamma"
    assert restored.fencing_token == 101


def test_lock_acquire_result():
    res = LockAcquireResult(
        acquired=True,
        resource="db_tx",
        fencing_token=7,
        holder_id="worker_1",
    )
    d = res.to_dict()
    restored = LockAcquireResult.from_dict(d)
    assert restored.acquired is True
    assert restored.fencing_token == 7
