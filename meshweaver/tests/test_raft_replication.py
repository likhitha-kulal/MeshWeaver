"""
Unit tests for Raft replication engine, AppendEntries processing, and quorum commits.
"""

import pytest
from meshweaver.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    LogEntry,
    Message,
    MessageType,
    RaftCommandType,
)
from meshweaver.raft_log import RaftLog, RaftReplicationEngine, ReplicatedStateMachine


def test_follower_append_entries_request_success():
    log = RaftLog()
    sm = ReplicatedStateMachine()
    engine = RaftReplicationEngine(
        node_id="follower_1",
        log=log,
        state_machine=sm,
        get_term_and_role_fn=lambda: (1, "FOLLOWER"),
    )

    req = AppendEntriesRequest(
        term=1,
        leader_id="leader_1",
        prev_log_index=0,
        prev_log_term=0,
        entries=[
            LogEntry(index=1, term=1, command_type=RaftCommandType.SET, key="alpha", value=100),
            LogEntry(index=2, term=1, command_type=RaftCommandType.SET, key="beta", value=200),
        ],
        leader_commit=1,
    )
    msg = Message(
        type=MessageType.RAFT_APPEND_ENTRIES_REQUEST,
        sender_id="leader_1",
        sender_udp_port=9000,
        payload=req.to_dict(),
    )

    resp_msg = engine.handle_append_entries_request(msg, ("127.0.0.1", 9000))
    assert resp_msg is not None
    resp = AppendEntriesResponse.from_dict(resp_msg.payload)
    assert resp.success is True
    assert resp.match_index == 2
    assert log.last_index == 2
    assert log.commit_index == 1
    assert sm.get("alpha") == 100
    assert sm.get("beta") is None  # beta not committed yet (commit_index = 1)


def test_follower_append_entries_stale_term_rejected():
    log = RaftLog()
    sm = ReplicatedStateMachine()
    engine = RaftReplicationEngine(
        node_id="follower_1",
        log=log,
        state_machine=sm,
        get_term_and_role_fn=lambda: (3, "FOLLOWER"),
    )

    req = AppendEntriesRequest(
        term=2,  # Lower than follower's term (3)
        leader_id="old_leader",
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0,
    )
    msg = Message(
        type=MessageType.RAFT_APPEND_ENTRIES_REQUEST,
        sender_id="old_leader",
        sender_udp_port=9000,
        payload=req.to_dict(),
    )

    resp_msg = engine.handle_append_entries_request(msg, ("127.0.0.1", 9000))
    resp = AppendEntriesResponse.from_dict(resp_msg.payload)
    assert resp.success is False
    assert resp.term == 3


def test_leader_quorum_commit_advancement():
    log = RaftLog()
    sm = ReplicatedStateMachine()
    engine = RaftReplicationEngine(
        node_id="leader_node",
        log=log,
        state_machine=sm,
        get_active_peers_fn=lambda: [("127.0.0.1", 9001), ("127.0.0.1", 9002)],
        get_term_and_role_fn=lambda: (1, "LEADER"),
    )

    # Leader appends 2 entries
    e1 = log.append_command(term=1, command_type=RaftCommandType.SET, key="k1", value="v1")
    e2 = log.append_command(term=1, command_type=RaftCommandType.SET, key="k2", value="v2")

    # Cluster has 3 nodes total (leader + 2 peers) -> Quorum required is 2
    assert log.commit_index == 0

    # Peer 1 acknowledges up to index 2
    resp1 = AppendEntriesResponse(term=1, follower_id="peer_1", success=True, match_index=2, last_log_index=2)
    msg1 = Message(
        type=MessageType.RAFT_APPEND_ENTRIES_RESPONSE,
        sender_id="peer_1",
        sender_udp_port=9001,
        payload=resp1.to_dict(),
    )
    engine.handle_append_entries_response(msg1)

    # Leader + Peer 1 = 2 nodes >= quorum -> commit index advances to 2!
    assert log.commit_index == 2
    assert log.last_applied == 2
    assert sm.get("k1") == "v1"
    assert sm.get("k2") == "v2"


@pytest.mark.asyncio
async def test_single_node_proposal_fast_path():
    log = RaftLog()
    sm = ReplicatedStateMachine()
    engine = RaftReplicationEngine(
        node_id="solo_node",
        log=log,
        state_machine=sm,
        get_active_peers_fn=lambda: [],
        get_term_and_role_fn=lambda: (1, "LEADER"),
    )

    result = await engine.propose_command(
        command_type=RaftCommandType.SET,
        key="config_key",
        value=42,
    )
    assert result == 42
    assert sm.get("config_key") == 42
    assert log.commit_index == 1
