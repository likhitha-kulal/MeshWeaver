"""
MeshWeaver Task Scheduler Module.
Provides intelligent load-balanced task dispatching, retry policies,
and failover mechanisms across active mesh peer nodes.
"""

from dataclasses import dataclass, field
from enum import Enum
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("meshweaver.scheduler")


class SchedulingPolicy(str, Enum):
    """Supported task routing and worker selection policies."""
    LEAST_LOADED = "least_loaded"
    ROUND_ROBIN = "round_robin"
    POWER_OF_TWO_RANDOM = "power_of_two_random"
    LOCAL_FIRST = "local_first"


@dataclass
class WorkerCandidate:
    """Represents a potential remote compute worker and its telemetry."""
    node_id: str
    host: str
    tcp_port: int
    cpu_percent: float = 0.0
    ram_percent: float = 0.0
    pending_tasks: int = 0
    score: float = 0.0
    is_alive: bool = True



@dataclass
class RetryPolicy:
    """Configuration for handling remote dispatch failures and retries."""
    max_retries: int = 3
    backoff_factor: float = 0.5
    timeout_per_attempt: float = 5.0
    exclude_failed_nodes: bool = True


class LoadScorer:
    """Calculates load metrics for candidate compute workers."""

    def __init__(
        self,
        cpu_weight: float = 0.6,
        ram_weight: float = 0.4,
        pending_task_weight: float = 5.0,
    ):
        self.cpu_weight = cpu_weight
        self.ram_weight = ram_weight
        self.pending_task_weight = pending_task_weight

    def calculate_score(
        self,
        cpu_percent: float,
        ram_percent: float,
        pending_tasks: int = 0,
    ) -> float:
        """
        Compute weighted composite load index (lower is better/less busy).
        Formula: (cpu_w * CPU%) + (ram_w * RAM%) + (task_w * pending_tasks)
        """
        base_score = (self.cpu_weight * cpu_percent) + (self.ram_weight * ram_percent)
        penalty = self.pending_task_weight * max(0, pending_tasks)
        return round(base_score + penalty, 3)

    def is_overloaded(
        self,
        cpu_percent: float,
        ram_percent: float,
        cpu_threshold: float = 90.0,
        ram_threshold: float = 95.0,
    ) -> bool:
        """Return True if worker resource utilization exceeds critical thresholds."""
        return cpu_percent >= cpu_threshold or ram_percent >= ram_threshold


class TaskScheduler:
    """
    Coordinates intelligent remote task dispatching, load balancing,
    and automatic failover across mesh peers.
    """

    def __init__(
        self,
        local_node_id: str,
        gossip_manager: Optional[Any] = None,
        load_scorer: Optional[LoadScorer] = None,
        default_policy: SchedulingPolicy = SchedulingPolicy.LEAST_LOADED,
        default_retry_policy: Optional[RetryPolicy] = None,
    ):
        self.local_node_id = local_node_id
        self.gossip_manager = gossip_manager
        self.load_scorer = load_scorer or LoadScorer()
        self.default_policy = default_policy
        self.retry_policy = default_retry_policy or RetryPolicy()
        self._round_robin_index = 0
        self._active_tasks: Dict[str, int] = {}  # node_id -> active task count

    def get_active_candidates(
        self,
        exclude_node_ids: Optional[Set[str]] = None,
    ) -> List[WorkerCandidate]:
        """
        Retrieve alive peer candidates from GossipManager and compute their composite load scores.
        """
        if not self.gossip_manager:
            return []

        excluded = exclude_node_ids or set()
        candidates: List[WorkerCandidate] = []

        peers = self.gossip_manager.get_all_peers()
        for node_id, peer in peers.items():
            if node_id in excluded or node_id == self.local_node_id:
                continue
            if not peer.is_alive or peer.tcp_port is None:
                continue

            pending = self._active_tasks.get(node_id, 0)
            score = self.load_scorer.calculate_score(
                cpu_percent=peer.cpu_percent,
                ram_percent=peer.ram_percent,
                pending_tasks=pending,
            )

            candidates.append(
                WorkerCandidate(
                    node_id=node_id,
                    host=peer.host,
                    tcp_port=peer.tcp_port,
                    cpu_percent=peer.cpu_percent,
                    ram_percent=peer.ram_percent,
                    pending_tasks=pending,
                    score=score,
                    is_alive=peer.is_alive,
                )
            )

        return candidates

    def _select_least_loaded(self, candidates: List[WorkerCandidate]) -> Optional[WorkerCandidate]:
        """Select the candidate worker with the lowest composite load score."""
        if not candidates:
            return None
        return min(candidates, key=lambda c: c.score)





