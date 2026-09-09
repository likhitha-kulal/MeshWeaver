"""
Unit tests for Raft InstallSnapshot RPC protocol and state machine compaction/restore.
"""

import pytest

from meshweaver.models import (
    ConsensusJob,
    ConsensusJobStatus,
    DistributedLock,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    LogEntry,
    Message,
    MessageType,
    RaftCommandType,
)
from meshweaver.raft_log import RaftLog, RaftReplicationEngine, ReplicatedStateMachine


def test_install_snapshot_serialization():
    req = InstallSnapshotRequest(
        term=3,
        leader_id="node_leader_01",
        last_included_index=15,
        last_included_term=2,
        data={
            "state": {"mesh_key": "mesh_value", "counter": 42},
            "fencing_token_counter": 5,
        },
        done=True,
    )
    d = req.to_dict()
    assert d["term"] == 3
    assert d["last_included_index"] == 15
    assert d["data"]["state"]["mesh_key"] == "mesh_value"

    restored = InstallSnapshotRequest.from_dict(d)
    assert restored.term == 3
    assert restored.last_included_index == 15
    assert restored.last_included_term == 2
    assert restored.data["state"]["counter"] == 42
    assert restored.done is True


def test_install_snapshot_response_serialization():
    resp = InstallSnapshotResponse(
        term=4,
        follower_id="node_follower_02",
        success=True,
        match_index=20,
    )
    d = resp.to_dict()
    assert d["term"] == 4
    assert d["follower_id"] == "node_follower_02"
    assert d["success"] is True
    assert d["match_index"] == 20

    restored = InstallSnapshotResponse.from_dict(d)
    assert restored.term == 4
    assert restored.success is True
    assert restored.match_index == 20


def test_replicated_state_machine_snapshot_export_import():
    sm = ReplicatedStateMachine()

    # Apply SET and INCREMENT commands
    sm.apply_command(LogEntry(index=1, term=1, command_type=RaftCommandType.SET, key="alpha", value="val_1"))
    sm.apply_command(LogEntry(index=2, term=1, command_type=RaftCommandType.INCREMENT, key="cnt", extra_data={"delta": 10}))
    sm.apply_command(LogEntry(index=3, term=1, command_type=RaftCommandType.LOCK_ACQUIRE, key="res_A", client_id="client_1", extra_data={"ttl_seconds": 60.0}))
    sm.apply_command(LogEntry(index=4, term=1, command_type=RaftCommandType.JOB_SUBMIT, key="job_01", client_id="client_1", extra_data={"job_id": "job_01", "priority": 1}))
    sm.apply_command(LogEntry(index=5, term=1, command_type=RaftCommandType.MEMBERSHIP_CHANGE, key="node_A", extra_data={"action": "ADD"}))

    snapshot = sm.export_state()
    assert snapshot["state"]["alpha"] == "val_1"
    assert snapshot["state"]["cnt"] == 10
    assert "res_A" in snapshot["locks"]
    assert "job_01" in snapshot["jobs"]
    assert "node_A" in snapshot["cluster_members"]

    # Restore in clean state machine
    new_sm = ReplicatedStateMachine()
    new_sm.import_state(snapshot)

    assert new_sm.get("alpha") == "val_1"
    assert new_sm.get("cnt") == 10
    assert new_sm.get_lock("res_A") is not None
    assert new_sm.get_job("job_01") is not None
    assert new_sm.get_job("job_01").priority == 1
    assert "node_A" in new_sm.get_cluster_members()


def test_follower_handles_install_snapshot():
    follower_log = RaftLog()
    follower_sm = ReplicatedStateMachine()
    engine = RaftReplicationEngine(
        node_id="follower_01",
        log=follower_log,
        state_machine=follower_sm,
        get_term_and_role_fn=lambda: (2, "FOLLOWER"),
    )

    # Leader sends snapshot up to index 25, term 2
    snapshot_data = {
        "state": {"k1": "v1", "k2": "v2"},
        "locks": {},
        "jobs": {},
        "cluster_members": ["leader:9000", "follower_01:9000"],
        "fencing_token_counter": 10,
        "commands_applied": 25,
    }
    req = InstallSnapshotRequest(
        term=2,
        leader_id="leader",
        last_included_index=25,
        last_included_term=2,
        data=snapshot_data,
    )
    msg = Message(
        type=MessageType.RAFT_INSTALL_SNAPSHOT_REQUEST,
        sender_id="leader",
        sender_udp_port=9000,
        payload=req.to_dict(),
    )

    resp_msg = engine.handle_install_snapshot_request(msg, ("127.0.0.1", 9000))
    assert resp_msg is not None
    assert resp_msg.type == MessageType.RAFT_INSTALL_SNAPSHOT_RESPONSE

    resp = InstallSnapshotResponse.from_dict(resp_msg.payload)
    assert resp.success is True
    assert resp.match_index == 25

    # Check follower state restored
    assert follower_sm.get("k1") == "v1"
    assert follower_sm.get("k2") == "v2"
    assert follower_log.commit_index == 25
    assert follower_log.last_applied == 25
    assert engine.total_snapshots_installed == 1


def test_leader_tracks_install_snapshot_ack():
    leader_log = RaftLog()
    leader_sm = ReplicatedStateMachine()
    engine = RaftReplicationEngine(
        node_id="leader",
        log=leader_log,
        state_machine=leader_sm,
        get_term_and_role_fn=lambda: (2, "LEADER"),
    )
    engine.initialize_follower("follower_01")

    resp = InstallSnapshotResponse(
        term=2,
        follower_id="follower_01",
        success=True,
        match_index=50,
    )
    msg = Message(
        type=MessageType.RAFT_INSTALL_SNAPSHOT_RESPONSE,
        sender_id="follower_01",
        sender_udp_port=9010,
        payload=resp.to_dict(),
    )

    engine.handle_install_snapshot_response(msg)
    follower = engine.followers["follower_01"]
    assert follower.match_index == 50
    assert follower.next_index == 51
    assert engine.replication_successes == 1
