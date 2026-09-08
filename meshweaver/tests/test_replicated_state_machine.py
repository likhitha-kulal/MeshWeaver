"""
Unit tests for ReplicatedStateMachine deterministic operations, CAS, and DistributedLockManager.
"""

import time
import pytest
from meshweaver.models import LogEntry, RaftCommandType
from meshweaver.raft_log import ReplicatedStateMachine


def test_state_machine_crud():
    sm = ReplicatedStateMachine()
    e1 = LogEntry(index=1, term=1, command_type=RaftCommandType.SET, key="greeting", value="hello world")
    assert sm.apply_command(e1) == "hello world"
    assert sm.get("greeting") == "hello world"

    e2 = LogEntry(index=2, term=1, command_type=RaftCommandType.INCREMENT, key="counter", extra_data={"delta": 5})
    assert sm.apply_command(e2) == 5

    e3 = LogEntry(index=3, term=1, command_type=RaftCommandType.INCREMENT, key="counter", extra_data={"delta": 2})
    assert sm.apply_command(e3) == 7
    assert sm.get("counter") == 7

    e4 = LogEntry(index=4, term=1, command_type=RaftCommandType.DELETE, key="greeting")
    assert sm.apply_command(e4) == "hello world"
    assert sm.get("greeting") is None


def test_state_machine_cas():
    sm = ReplicatedStateMachine()
    sm.apply_command(LogEntry(index=1, term=1, command_type=RaftCommandType.SET, key="status", value="INIT"))

    cas_fail = LogEntry(
        index=2,
        term=1,
        command_type=RaftCommandType.CAS,
        key="status",
        value="RUNNING",
        extra_data={"expected": "READY"},
    )
    assert sm.apply_command(cas_fail) is False
    assert sm.get("status") == "INIT"

    cas_ok = LogEntry(
        index=3,
        term=1,
        command_type=RaftCommandType.CAS,
        key="status",
        value="RUNNING",
        extra_data={"expected": "INIT"},
    )
    assert sm.apply_command(cas_ok) is True
    assert sm.get("status") == "RUNNING"


def test_distributed_lock_acquisition_and_fencing():
    sm = ReplicatedStateMachine()
    # Node A acquires lock
    e1 = LogEntry(
        index=1,
        term=1,
        command_type=RaftCommandType.LOCK_ACQUIRE,
        key="file_mutex",
        client_id="node_a",
        extra_data={"ttl_seconds": 60.0},
    )
    res1 = sm.apply_command(e1)
    assert res1.acquired is True
    assert res1.fencing_token == 1
    assert res1.holder_id == "node_a"

    # Node B tries to acquire same lock -> fails
    e2 = LogEntry(
        index=2,
        term=1,
        command_type=RaftCommandType.LOCK_ACQUIRE,
        key="file_mutex",
        client_id="node_b",
        extra_data={"ttl_seconds": 60.0},
    )
    res2 = sm.apply_command(e2)
    assert res2.acquired is False
    assert res2.fencing_token is None

    # Node A releases lock
    e3 = LogEntry(
        index=3,
        term=1,
        command_type=RaftCommandType.LOCK_RELEASE,
        key="file_mutex",
        client_id="node_a",
        fencing_token=1,
    )
    assert sm.apply_command(e3) is True

    # Node B now acquires lock -> succeeds with fencing token 2
    res3 = sm.apply_command(e2)
    assert res3.acquired is True
    assert res3.fencing_token == 2
    assert res3.holder_id == "node_b"


def test_batch_atomic_operations():
    sm = ReplicatedStateMachine()
    batch_entry = LogEntry(
        index=1,
        term=1,
        command_type=RaftCommandType.BATCH,
        extra_data={
            "operations": [
                LogEntry(index=1, term=1, command_type=RaftCommandType.SET, key="a", value=10).to_dict(),
                LogEntry(index=2, term=1, command_type=RaftCommandType.SET, key="b", value=20).to_dict(),
                LogEntry(index=3, term=1, command_type=RaftCommandType.INCREMENT, key="a", extra_data={"delta": 5}).to_dict(),
            ]
        },
    )
    results = sm.apply_command(batch_entry)
    assert results == [10, 20, 15]
    assert sm.get("a") == 15
    assert sm.get("b") == 20


def test_state_snapshot_export_import():
    sm1 = ReplicatedStateMachine()
    sm1.apply_command(LogEntry(index=1, term=1, command_type=RaftCommandType.SET, key="k1", value="v1"))
    sm1.apply_command(LogEntry(index=2, term=1, command_type=RaftCommandType.LOCK_ACQUIRE, key="res1", client_id="c1"))

    snapshot = sm1.export_state()
    sm2 = ReplicatedStateMachine()
    sm2.import_state(snapshot)

    assert sm2.get("k1") == "v1"
    assert sm2.get_lock("res1") is not None
    assert sm2.get_lock("res1").holder_id == "c1"
