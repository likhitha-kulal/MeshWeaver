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
