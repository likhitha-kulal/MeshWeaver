"""
Unit tests for MeshWeaver Leader Election & Consensus Engine.
"""

import pytest
import time
from meshweaver.models import (
    ElectionRole,
    LeaderHeartbeat,
    LeaderHeartbeatAck,
    Message,
    MessageType,
    VoteRequest,
    VoteResponse,
)


def test_election_models_serialization():
    req = VoteRequest(term=3, candidate_id="cand_1", last_log_index=10, last_log_term=2)
    d = req.to_dict()
    assert d["term"] == 3
    assert d["candidate_id"] == "cand_1"
    req_restored = VoteRequest.from_dict(d)
    assert req_restored.term == 3
    assert req_restored.candidate_id == "cand_1"

    resp = VoteResponse(term=3, vote_granted=True, voter_id="voter_1")
    resp_restored = VoteResponse.from_dict(resp.to_dict())
    assert resp_restored.vote_granted is True

    hb = LeaderHeartbeat(term=4, leader_id="leader_a", lease_duration=0.5)
    hb_restored = LeaderHeartbeat.from_dict(hb.to_dict())
    assert hb_restored.lease_duration == 0.5

    ack = LeaderHeartbeatAck(term=4, node_id="node_b", accepted=True)
    ack_restored = LeaderHeartbeatAck.from_dict(ack.to_dict())
    assert ack_restored.accepted is True


@pytest.mark.asyncio
async def test_election_state_and_config():
    config = ElectionConfig(min_election_timeout=0.1, max_election_timeout=0.2)
    engine = LeaderElectionEngine(node_id="node_1", config=config)
    assert engine.role == ElectionRole.FOLLOWER
    assert not engine.is_leader
    assert engine.current_term == 0
    assert engine.current_leader is None
    assert 0.1 <= engine.state.election_timeout <= 0.2


@pytest.mark.asyncio
async def test_single_node_election_fast_path():
    engine = LeaderElectionEngine(node_id="node_solo", get_active_peers_fn=lambda: [])
    await engine.start_election()
    assert engine.role == ElectionRole.LEADER
    assert engine.is_leader
    assert engine.current_leader == "node_solo"
    assert engine.current_term == 1
    assert engine.elections_won == 1
    await engine.stop()


@pytest.mark.asyncio
async def test_vote_request_and_response():
    engine = LeaderElectionEngine(node_id="voter_1")
    
    req = VoteRequest(term=1, candidate_id="candidate_a")
    msg = Message(
        type=MessageType.ELECTION_VOTE_REQUEST,
        sender_id="candidate_a",
        sender_udp_port=9000,
        payload=req.to_dict(),
    )
    
    resp_msg = engine.handle_vote_request(msg, ("127.0.0.1", 9000))
    assert resp_msg is not None
    resp = VoteResponse.from_dict(resp_msg.payload)
    assert resp.vote_granted is True
    assert resp.term == 1
    assert resp.voter_id == "voter_1"
    assert engine.state.voted_for == "candidate_a"

    req2 = VoteRequest(term=1, candidate_id="candidate_b")
    msg2 = Message(
        type=MessageType.ELECTION_VOTE_REQUEST,
        sender_id="candidate_b",
        sender_udp_port=9001,
        payload=req2.to_dict(),
    )
    resp_msg2 = engine.handle_vote_request(msg2, ("127.0.0.1", 9001))
    resp2 = VoteResponse.from_dict(resp_msg2.payload)
    assert resp2.vote_granted is False


@pytest.mark.asyncio
async def test_candidate_quorum_tally():
    peers = [("127.0.0.1", 9001), ("127.0.0.1", 9002)]
    sent_messages = []
    
    def send_fn(h, p, m):
        sent_messages.append((h, p, m))
        
    engine = LeaderElectionEngine(
        node_id="candidate_node",
        get_active_peers_fn=lambda: peers,
        send_message_fn=send_fn,
    )
    
    await engine.start_election()
    assert engine.role == ElectionRole.CANDIDATE
    assert engine.current_term == 1
    assert len(sent_messages) == 2
    
    vote_resp = VoteResponse(term=1, vote_granted=True, voter_id="peer_1")
    resp_msg = Message(
        type=MessageType.ELECTION_VOTE_RESPONSE,
        sender_id="peer_1",
        sender_udp_port=9001,
        payload=vote_resp.to_dict(),
    )
    
    await engine.handle_vote_response(resp_msg)
    assert engine.role == ElectionRole.LEADER
    assert engine.is_leader
    assert engine.current_leader == "candidate_node"
    await engine.stop()


@pytest.mark.asyncio
async def test_step_down_on_higher_term():
    engine = LeaderElectionEngine(node_id="node_term_test")
    engine.state.role = ElectionRole.LEADER
    engine.state.current_term = 2
    
    req = VoteRequest(term=5, candidate_id="newer_leader")
    msg = Message(
        type=MessageType.ELECTION_VOTE_REQUEST,
        sender_id="newer_leader",
        sender_udp_port=9000,
        payload=req.to_dict(),
    )
    
    engine.handle_vote_request(msg, ("127.0.0.1", 9000))
    assert engine.role == ElectionRole.FOLLOWER
    assert engine.current_term == 5
    assert engine.state.voted_for == "newer_leader"


@pytest.mark.asyncio
async def test_leader_heartbeat_and_lease():
    engine = LeaderElectionEngine(node_id="follower_1")
    
    hb = LeaderHeartbeat(term=2, leader_id="leader_x", lease_duration=0.5)
    hb_msg = Message(
        type=MessageType.LEADER_HEARTBEAT,
        sender_id="leader_x",
        sender_udp_port=9000,
        payload=hb.to_dict(),
    )
    
    ack_msg = engine.handle_leader_heartbeat(hb_msg, ("127.0.0.1", 9000))
    assert ack_msg is not None
    ack = LeaderHeartbeatAck.from_dict(ack_msg.payload)
    assert ack.accepted is True
    assert engine.current_leader == "leader_x"
    assert engine.current_term == 2
    assert engine.state.lease_expires_at > time.time()


@pytest.mark.asyncio
async def test_consensus_metrics():
    engine = LeaderElectionEngine(node_id="metrics_node", get_active_peers_fn=lambda: [])
    await engine.start_election()
    metrics = engine.get_consensus_metrics()
    assert isinstance(metrics, ConsensusMetrics)
    assert metrics.is_leader is True
    assert metrics.current_term == 1
    assert metrics.elections_won == 1
    metrics_dict = metrics.to_dict()
    assert metrics_dict["node_id"] == "metrics_node"
    assert metrics_dict["role"] == "LEADER"
    await engine.stop()
