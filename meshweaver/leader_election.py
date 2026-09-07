"""
MeshWeaver Distributed Leader Election & Consensus Engine
Implements randomized lease-based leader election, heartbeat lease renewal,
split-brain quorum enforcement, dynamic failover, and candidate term preemption.
"""

import asyncio
from dataclasses import dataclass, field
import logging
import random
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from meshweaver.models import (
    ElectionRole,
    LeaderHeartbeat,
    LeaderHeartbeatAck,
    Message,
    MessageType,
    VoteRequest,
    VoteResponse,
)

logger = logging.getLogger("meshweaver.consensus")


@dataclass
class ElectionConfig:
    """Configuration parameters for leader election timing and lease dynamics."""
    min_election_timeout: float = 0.200  # 200ms
    max_election_timeout: float = 0.400  # 400ms
    heartbeat_interval: float = 0.080    # 80ms
    lease_duration: float = 0.500        # 500ms
    auto_election: bool = True
    min_quorum: int = 1                  # Minimum votes required if cluster size is 1


@dataclass
class ElectionState:
    """Tracks persistent and volatile state across election terms."""
    role: ElectionRole = ElectionRole.FOLLOWER
    current_term: int = 0
    voted_for: Optional[str] = None
    current_leader: Optional[str] = None
    votes_received: Set[str] = field(default_factory=set)
    last_heartbeat_received: float = field(default_factory=time.time)
    lease_expires_at: float = 0.0
    election_timeout: float = 0.300



@dataclass
class ConsensusMetrics:
    """Real-time observability snapshot for leader election metrics."""
    node_id: str
    role: str
    current_term: int
    current_leader: Optional[str]
    is_leader: bool
    elections_started: int
    elections_won: int
    terms_served: int
    total_votes_requested: int
    total_votes_granted: int
    heartbeats_sent: int
    heartbeats_received: int
    last_election_duration_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "role": self.role,
            "current_term": self.current_term,
            "current_leader": self.current_leader,
            "is_leader": self.is_leader,
            "elections_started": self.elections_started,
            "elections_won": self.elections_won,
            "terms_served": self.terms_served,
            "total_votes_requested": self.total_votes_requested,
            "total_votes_granted": self.total_votes_granted,
            "heartbeats_sent": self.heartbeats_sent,
            "heartbeats_received": self.heartbeats_received,
            "last_election_duration_ms": self.last_election_duration_ms,
        }


class LeaderElectionEngine:
    """
    Decentralized Leader Election Engine implementing randomized timeouts,
    term-based voting, quorum promotion, and heartbeat lease renewals.
    """

    def __init__(
        self,
        node_id: str,
        config: Optional[ElectionConfig] = None,
        get_active_peers_fn: Optional[Callable[[], List[Tuple[str, int]]]] = None,
        send_message_fn: Optional[Callable[[str, int, Message], None]] = None,
    ):
        self.node_id = node_id
        self.config = config or ElectionConfig()
        self.get_active_peers = get_active_peers_fn or (lambda: [])
        self.send_message = send_message_fn or (lambda host, port, msg: None)

        self.state = ElectionState()
        self._reset_election_timeout()
        self._running = False
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        # Metrics
        self.elections_started = 0
        self.elections_won = 0
        self.terms_served = 0
        self.total_votes_requested = 0
        self.total_votes_granted = 0
        self.heartbeats_sent = 0
        self.heartbeats_received = 0
        self.last_election_duration_ms = 0.0

        # Callbacks
        self._on_leader_elected_callbacks: List[Callable[[str, int], None]] = []
        self._on_step_down_callbacks: List[Callable[[int], None]] = []

    @property
    def role(self) -> ElectionRole:
        return self.state.role

    @property
    def is_leader(self) -> bool:
        return self.state.role == ElectionRole.LEADER

    @property
    def current_term(self) -> int:
        return self.state.current_term

    @property
    def current_leader(self) -> Optional[str]:
        return self.state.current_leader

    def _reset_election_timeout(self) -> None:
        """Calculate a randomized jittered election timeout between min and max bounds."""
        self.state.election_timeout = random.uniform(
            self.config.min_election_timeout,
            self.config.max_election_timeout,
        )
        self.state.last_heartbeat_received = time.time()

    async def start(self) -> None:
        """Start the background consensus timer loop."""
        if self._running:
            return
        self._running = True
        self._reset_election_timeout()
        if self.config.auto_election:
            self._election_task = asyncio.create_task(self._election_timer_loop())
        logger.info(f"LeaderElectionEngine started for node {self.node_id[:8]} in term {self.state.current_term}")

    async def stop(self) -> None:
        """Gracefully stop background consensus tasks."""
        self._running = False
        if self._election_task:
            self._election_task.cancel()
            try:
                await self._election_task
            except asyncio.CancelledError:
                pass
            self._election_task = None

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        logger.info(f"LeaderElectionEngine stopped for node {self.node_id[:8]}")

    async def _election_timer_loop(self) -> None:
        """Periodic background evaluation checking for leader heartbeat timeouts."""
        while self._running:
            try:
                await asyncio.sleep(0.020)
                now = time.time()
                elapsed = now - self.state.last_heartbeat_received

                if self.state.role == ElectionRole.FOLLOWER:
                    if elapsed > self.state.election_timeout:
                        logger.warning(
                            f"Node {self.node_id[:8]} election timeout ({elapsed:.3f}s > {self.state.election_timeout:.3f}s). "
                            f"Starting election."
                        )
                        await self.start_election()

                elif self.state.role == ElectionRole.CANDIDATE:
                    if elapsed > self.state.election_timeout:
                        logger.warning(
                            f"Node {self.node_id[:8]} candidate timeout in term {self.state.current_term}. Restarting election."
                        )
                        await self.start_election()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in election timer loop: {e}", exc_info=True)

    async def start_election(self) -> None:
        """Transition to CANDIDATE, increment term, vote for self, and broadcast VoteRequests."""
        start_ts = time.time()
        self.state.role = ElectionRole.CANDIDATE
        self.state.current_term += 1
        self.state.voted_for = self.node_id
        self.state.votes_received = {self.node_id}
        self.state.current_leader = None
        self.elections_started += 1
        self._reset_election_timeout()

        active_peers = self.get_active_peers()
        total_cluster_size = len(active_peers) + 1  # Peers + self
        required_quorum = (total_cluster_size // 2) + 1

        logger.info(
            f"Node {self.node_id[:8]} started election for Term {self.state.current_term}. "
            f"Peers: {len(active_peers)}, Required Quorum: {required_quorum}"
        )

        # Single-node cluster fast-path
        if total_cluster_size == 1 or len(active_peers) == 0:
            await self._promote_to_leader(start_ts)
            return

        vote_req = VoteRequest(
            term=self.state.current_term,
            candidate_id=self.node_id,
        )
        msg = Message(
            type=MessageType.ELECTION_VOTE_REQUEST,
            sender_id=self.node_id,
            sender_udp_port=0,
            payload=vote_req.to_dict(),
        )

        self.total_votes_requested += len(active_peers)
        for host, port in active_peers:
            try:
                self.send_message(host, port, msg)
            except Exception as e:
                logger.debug(f"Failed to dispatch vote request to {host}:{port}: {e}")

    def handle_vote_request(self, msg: Message, addr: Tuple[str, int]) -> Optional[Message]:
        """Evaluate incoming VoteRequest and return VoteResponse."""
        try:
            req = VoteRequest.from_dict(msg.payload)
        except Exception as e:
            logger.warning(f"Malformed VoteRequest received: {e}")
            return None

        # Rule 1: If term < current_term, reject vote
        if req.term < self.state.current_term:
            resp = VoteResponse(
                term=self.state.current_term,
                vote_granted=False,
                voter_id=self.node_id,
            )
            return Message(
                msg_id=msg.msg_id,
                type=MessageType.ELECTION_VOTE_RESPONSE,
                sender_id=self.node_id,
                sender_udp_port=0,
                payload=resp.to_dict(),
            )

        # Rule 2: If term > current_term, update term and step down to follower
        if req.term > self.state.current_term:
            self.step_down(req.term)

        # Rule 3: Grant vote if not yet voted for another candidate in this term
        can_vote = (self.state.voted_for is None or self.state.voted_for == req.candidate_id)
        if can_vote:
            self.state.voted_for = req.candidate_id
            self.state.last_heartbeat_received = time.time()
            self.total_votes_granted += 1
            logger.info(f"Node {self.node_id[:8]} granted vote to {req.candidate_id[:8]} for Term {req.term}")

        resp = VoteResponse(
            term=self.state.current_term,
            vote_granted=can_vote,
            voter_id=self.node_id,
        )
        return Message(
            msg_id=msg.msg_id,
            type=MessageType.ELECTION_VOTE_RESPONSE,
            sender_id=self.node_id,
            sender_udp_port=0,
            payload=resp.to_dict(),
        )

    async def handle_vote_response(self, msg: Message) -> None:
        """Process incoming VoteResponse and tally quorum for leader promotion."""
        if msg.type != MessageType.ELECTION_VOTE_RESPONSE or "vote_granted" not in msg.payload:
            return
        try:
            resp = VoteResponse.from_dict(msg.payload)
        except Exception as e:
            logger.warning(f"Malformed VoteResponse received: {e}")
            return

        # If higher term discovered, immediately step down
        if resp.term > self.state.current_term:
            self.step_down(resp.term)
            return

        if self.state.role != ElectionRole.CANDIDATE or resp.term != self.state.current_term:
            return

        if resp.vote_granted:
            self.state.votes_received.add(resp.voter_id)
            active_peers = self.get_active_peers()
            total_cluster_size = len(active_peers) + 1
            required_quorum = (total_cluster_size // 2) + 1

            logger.info(
                f"Node {self.node_id[:8]} received vote from {resp.voter_id[:8]}. "
                f"Tally: {len(self.state.votes_received)}/{required_quorum}"
            )

            if len(self.state.votes_received) >= required_quorum:
                await self._promote_to_leader(time.time() - 0.05)

    async def _promote_to_leader(self, start_ts: float) -> None:
        """Promote node to LEADER role and launch periodic heartbeat lease broadcaster."""
        if self.state.role == ElectionRole.LEADER:
            return

        self.state.role = ElectionRole.LEADER
        self.state.current_leader = self.node_id
        self.elections_won += 1
        self.terms_served += 1
        self.last_election_duration_ms = (time.time() - start_ts) * 1000.0

        logger.info(
            f"👑 Node {self.node_id[:8]} PROMOTED TO LEADER for Term {self.state.current_term}! "
            f"Duration: {self.last_election_duration_ms:.2f}ms"
        )

        for cb in self._on_leader_elected_callbacks:
            try:
                cb(self.node_id, self.state.current_term)
            except Exception as e:
                logger.error(f"Error in leader elected callback: {e}")

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._leader_heartbeat_loop())

    async def _leader_heartbeat_loop(self) -> None:
        """Broadcasts periodic heartbeat leases to all cluster peers."""
        while self._running and self.state.role == ElectionRole.LEADER:
            try:
                self.broadcast_heartbeat()
                await asyncio.sleep(self.config.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in leader heartbeat loop: {e}", exc_info=True)

    def broadcast_heartbeat(self) -> None:
        """Send a LeaderHeartbeat message to all active peers."""
        if self.state.role != ElectionRole.LEADER:
            return

        heartbeat = LeaderHeartbeat(
            term=self.state.current_term,
            leader_id=self.node_id,
            lease_duration=self.config.lease_duration,
        )
        msg = Message(
            type=MessageType.LEADER_HEARTBEAT,
            sender_id=self.node_id,
            sender_udp_port=0,
            payload=heartbeat.to_dict(),
        )

        active_peers = self.get_active_peers()
        for host, port in active_peers:
            try:
                self.send_message(host, port, msg)
                self.heartbeats_sent += 1
            except Exception as e:
                logger.debug(f"Failed to send heartbeat to {host}:{port}: {e}")

    def handle_leader_heartbeat(self, msg: Message, addr: Tuple[str, int]) -> Optional[Message]:
        """Process incoming LeaderHeartbeat from cluster leader."""
        try:
            hb = LeaderHeartbeat.from_dict(msg.payload)
        except Exception as e:
            logger.warning(f"Malformed LeaderHeartbeat received: {e}")
            return None

        # Rule 1: If term < current_term, reject heartbeat
        if hb.term < self.state.current_term:
            ack = LeaderHeartbeatAck(term=self.state.current_term, node_id=self.node_id, accepted=False)
            return Message(
                msg_id=msg.msg_id,
                type=MessageType.LEADER_HEARTBEAT_ACK,
                sender_id=self.node_id,
                sender_udp_port=0,
                payload=ack.to_dict(),
            )

        # Rule 2: If term >= current_term, acknowledge leader and reset timer
        if hb.term > self.state.current_term:
            self.step_down(hb.term)
        elif self.state.role == ElectionRole.CANDIDATE:
            self.step_down(hb.term)

        self.state.current_leader = hb.leader_id
        self.state.last_heartbeat_received = time.time()
        self.state.lease_expires_at = time.time() + hb.lease_duration
        self.heartbeats_received += 1

        ack = LeaderHeartbeatAck(term=self.state.current_term, node_id=self.node_id, accepted=True)
        return Message(
            msg_id=msg.msg_id,
            type=MessageType.LEADER_HEARTBEAT_ACK,
            sender_id=self.node_id,
            sender_udp_port=0,
            payload=ack.to_dict(),
        )

    def step_down(self, new_term: int) -> None:
        """Demote to FOLLOWER on observing higher term or cluster abdication."""
        old_role = self.state.role
        self.state.role = ElectionRole.FOLLOWER
        self.state.current_term = max(self.state.current_term, new_term)
        self.state.voted_for = None
        self.state.votes_received.clear()
        self._reset_election_timeout()

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

        if old_role == ElectionRole.LEADER:
            logger.warning(f"Leader {self.node_id[:8]} stepped down to FOLLOWER in Term {self.state.current_term}")
            for cb in self._on_step_down_callbacks:
                try:
                    cb(self.state.current_term)
                except Exception as e:
                    logger.error(f"Error in step down callback: {e}")

    def add_leader_elected_callback(self, cb: Callable[[str, int], None]) -> None:
        self._on_leader_elected_callbacks.append(cb)

    def add_step_down_callback(self, cb: Callable[[int], None]) -> None:
        self._on_step_down_callbacks.append(cb)

    def trigger_election(self) -> None:
        """Manually trigger an immediate election cycle."""
        asyncio.create_task(self.start_election())

    def get_consensus_metrics(self) -> ConsensusMetrics:
        """Capture real-time consensus telemetry snapshot."""
        return ConsensusMetrics(
            node_id=self.node_id,
            role=self.state.role.value,
            current_term=self.state.current_term,
            current_leader=self.state.current_leader,
            is_leader=self.is_leader,
            elections_started=self.elections_started,
            elections_won=self.elections_won,
            terms_served=self.terms_served,
            total_votes_requested=self.total_votes_requested,
            total_votes_granted=self.total_votes_granted,
            heartbeats_sent=self.heartbeats_sent,
            heartbeats_received=self.heartbeats_received,
            last_election_duration_ms=self.last_election_duration_ms,
        )
