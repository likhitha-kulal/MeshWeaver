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


