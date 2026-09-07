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
