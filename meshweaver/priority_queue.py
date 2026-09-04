"""
MeshWeaver Priority Task Queue & QoS Management Module.
Provides multi-tier prioritized task scheduling, starvation-free dynamic aging,
deadline-aware priority promotion, and QoS telemetry across the mesh compute cluster.
"""

from dataclasses import dataclass, field
from enum import IntEnum
import time
from typing import Any, Callable, Dict, List, Optional, Tuple


class TaskPriority(IntEnum):
    """
    QoS Priority levels for MeshWeaver tasks (lower integer = higher execution precedence).
    """
    CRITICAL = 0    # Urgent control, cluster coordination, immediate health tasks
    HIGH = 1        # Interactive user requests, time-sensitive queries
    NORMAL = 2      # Standard background and ad-hoc compute tasks (default)
    LOW = 3         # Batch processing, non-urgent ETL pipelines
    BACKGROUND = 4  # Speculative computations, cache pre-warming, routine indexing


@dataclass
class PriorityMetrics:
    """Telemetry capturing QoS and wait-time metrics for prioritized tasks."""
    total_enqueued: int = 0
    total_completed: int = 0
    total_failed: int = 0
    total_cancelled: int = 0
    total_aged_promotions: int = 0
    total_deadline_promotions: int = 0
    avg_wait_time_ms: float = 0.0
    tasks_by_priority: Dict[int, int] = field(default_factory=lambda: {p.value: 0 for p in TaskPriority})


@dataclass(order=False)
class PrioritizedTask:
    """
    Represents a task queued with specific priority and scheduling metadata.
    """
    task_id: str
    func: Callable[..., Any]
    args: Tuple[Any, ...] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    base_priority: TaskPriority = TaskPriority.NORMAL
    created_at: float = field(default_factory=time.time)
    deadline: Optional[float] = None
    timeout: Optional[float] = None
    future: Optional[Any] = None  # asyncio.Future
    sequence: int = 0
    assigned_worker: Optional[str] = None
    cancelled: bool = False

    @property
    def age_seconds(self) -> float:
        """Elapsed time since task was enqueued."""
        return max(0.0, time.time() - self.created_at)

    def calculate_effective_priority(
        self,
        aging_interval_seconds: float = 2.0,
        deadline_boost_weight: float = 2.0,
    ) -> float:
        """
        Calculates dynamic effective priority score (lower is higher priority).
        Formula:
            P_eff = base_priority - (age / aging_interval) - deadline_urgency
        """
        score = float(self.base_priority.value)

        # 1. Starvation prevention: promote priority based on waiting duration
        if aging_interval_seconds > 0:
            promotions = self.age_seconds / aging_interval_seconds
            score -= promotions

        # 2. Deadline awareness: increase priority as deadline approaches
        if self.deadline is not None:
            remaining = self.deadline - time.time()
            if remaining <= 0:
                # Past deadline: maximum promotion
                score -= deadline_boost_weight * 2.0
            elif remaining < 5.0:
                # Approaching deadline within 5s
                urgency = (5.0 - remaining) / 5.0
                score -= deadline_boost_weight * urgency

        return score

    def __lt__(self, other: "PrioritizedTask") -> bool:
        """Compare two tasks by effective priority, breaking ties using sequence (FIFO)."""
        if not isinstance(other, PrioritizedTask):
            return NotImplemented
        self_p = self.calculate_effective_priority()
        other_p = other.calculate_effective_priority()
        if abs(self_p - other_p) > 1e-6:
            return self_p < other_p
        return self.sequence < other.sequence
