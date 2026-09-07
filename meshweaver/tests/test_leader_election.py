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
