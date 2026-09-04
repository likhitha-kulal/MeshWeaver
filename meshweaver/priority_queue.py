"""
MeshWeaver Priority Task Queue & QoS Management Module.
Provides multi-tier prioritized task scheduling, starvation-free dynamic aging,
deadline-aware priority promotion, and QoS telemetry across the mesh compute cluster.
"""

import asyncio
from dataclasses import dataclass, field
from enum import IntEnum
import heapq
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("meshweaver.priority_queue")


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


def calculate_deadline_urgency(
    deadline: Optional[float],
    current_time: Optional[float] = None,
    deadline_boost_weight: float = 2.0,
    critical_threshold_seconds: float = 5.0,
) -> float:
    """
    Computes priority promotion offset for tasks with impending deadlines.
    Returns the numeric boost (subtracted from priority score).
    """
    if deadline is None:
        return 0.0

    now = current_time if current_time is not None else time.time()
    remaining = deadline - now

    if remaining <= 0:
        # Overdue: maximum boost
        return deadline_boost_weight * 2.0
    elif remaining < critical_threshold_seconds:
        urgency_factor = (critical_threshold_seconds - remaining) / critical_threshold_seconds
        return deadline_boost_weight * urgency_factor
    return 0.0


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
    aging_interval_seconds: float = 2.0
    deadline_boost_weight: float = 2.0

    @property
    def age_seconds(self) -> float:
        """Elapsed time since task was enqueued."""
        return max(0.0, time.time() - self.created_at)

    def calculate_effective_priority(
        self,
        aging_interval_seconds: Optional[float] = None,
        deadline_boost_weight: Optional[float] = None,
        current_time: Optional[float] = None,
    ) -> float:
        """
        Calculates dynamic effective priority score (lower is higher priority).
        Formula:
            P_eff = base_priority - (age / aging_interval) - deadline_urgency
        """
        score = float(self.base_priority.value)
        now = current_time if current_time is not None else time.time()
        aging_int = self.aging_interval_seconds if aging_interval_seconds is None else aging_interval_seconds
        dl_weight = self.deadline_boost_weight if deadline_boost_weight is None else deadline_boost_weight

        # 1. Starvation prevention: promote priority based on waiting duration
        if aging_int > 0:
            age = max(0.0, now - self.created_at)
            promotions = age / aging_int
            score -= promotions

        # 2. Deadline awareness: increase priority as deadline approaches
        if self.deadline is not None:
            boost = calculate_deadline_urgency(
                deadline=self.deadline,
                current_time=now,
                deadline_boost_weight=dl_weight,
            )
            score -= boost

        return score


    def __lt__(self, other: "PrioritizedTask") -> bool:
        """Compare two tasks by effective priority, breaking ties using sequence (FIFO)."""
        if not isinstance(other, PrioritizedTask):
            return NotImplemented
        now = time.time()
        self_p = self.calculate_effective_priority(current_time=now)
        other_p = other.calculate_effective_priority(current_time=now)
        if abs(self_p - other_p) > 1e-4:
            return self_p < other_p
        return self.sequence < other.sequence



class PriorityTaskQueue:
    """
    Thread-safe & async-compatible priority queue for compute tasks.
    Maintains a min-heap structure ordered by dynamic effective priority.
    """

    def __init__(
        self,
        aging_interval_seconds: float = 2.0,
        deadline_boost_weight: float = 2.0,
        maxsize: int = 0,
    ):
        self.aging_interval_seconds = aging_interval_seconds
        self.deadline_boost_weight = deadline_boost_weight
        self.maxsize = maxsize
        self._heap: List[PrioritizedTask] = []
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition(self._lock)
        self._sequence_counter = 0
        self._tasks_by_id: Dict[str, PrioritizedTask] = {}
        self.metrics = PriorityMetrics()

    def qsize(self) -> int:
        """Return the current number of tasks in the queue."""
        return len(self._heap)

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._heap) == 0

    async def push(self, task: PrioritizedTask) -> None:
        """
        Enqueue a new prioritized task and notify waiting workers.
        """
        async with self._not_empty:
            self._sequence_counter += 1
            task.sequence = self._sequence_counter
            task.aging_interval_seconds = self.aging_interval_seconds
            task.deadline_boost_weight = self.deadline_boost_weight
            heapq.heappush(self._heap, task)
            self._tasks_by_id[task.task_id] = task

            # Update metrics
            self.metrics.total_enqueued += 1
            p_val = task.base_priority.value
            self.metrics.tasks_by_priority[p_val] = self.metrics.tasks_by_priority.get(p_val, 0) + 1

            self._not_empty.notify()


    async def pop(self) -> PrioritizedTask:
        """
        Retrieve and remove the highest priority task, waiting if empty.
        Re-heapifies to ensure aging calculations reflect current timestamp.
        """
        async with self._not_empty:
            while self.is_empty():
                await self._not_empty.wait()

            # Refresh heap ordering with dynamic aging
            self._reheapify()
            task = heapq.heappop(self._heap)
            self._tasks_by_id.pop(task.task_id, None)
            return task

    def _reheapify(self) -> None:
        """Re-orders the underlying heap based on current effective priority."""
        for t in self._heap:
            old_p = t.base_priority.value
            new_p = t.calculate_effective_priority(
                aging_interval_seconds=self.aging_interval_seconds,
                deadline_boost_weight=self.deadline_boost_weight,
            )
            if new_p < old_p:
                self.metrics.total_aged_promotions += 1
        heapq.heapify(self._heap)

    def peek(self) -> Optional[PrioritizedTask]:
        """Inspect the highest priority task without removing it."""
        if not self._heap:
            return None
        self._reheapify()
        return self._heap[0]

    def get_effective_queue_snapshot(self) -> List[Dict[str, Any]]:
        """Return a sorted list of all waiting tasks with their effective priorities and wait durations."""
        self._reheapify()
        snapshot = []
        for t in sorted(self._heap):
            snapshot.append({
                "task_id": t.task_id,
                "base_priority": t.base_priority.name,
                "effective_priority": round(t.calculate_effective_priority(self.aging_interval_seconds, self.deadline_boost_weight), 2),
                "wait_time_seconds": round(t.age_seconds, 2),
                "deadline": t.deadline,
            })
        return snapshot


class PriorityDispatcher:
    """
    Asynchronous QoS task dispatcher.
    Consumes tasks from PriorityTaskQueue and orchestrates execution across worker mesh nodes.
    """

    def __init__(
        self,
        scheduler: Any,
        concurrency: int = 5,
        queue: Optional[PriorityTaskQueue] = None,
    ):
        self.scheduler = scheduler
        self.concurrency = max(1, concurrency)
        self.queue = queue or PriorityTaskQueue()
        self._workers: List[asyncio.Task] = []
        self._running = False
        self._total_wait_time_ms = 0.0
        self._processed_tasks_count = 0

    @property
    def metrics(self) -> PriorityMetrics:
        """Access QoS metrics."""
        return self.queue.metrics

    async def start(self) -> None:
        """Start background worker pool."""
        if self._running:
            return
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker_loop(i))
            for i in range(self.concurrency)
        ]
        logger.debug(f"Started {self.concurrency} priority dispatcher worker(s).")

    async def stop(self) -> None:
        """Stop background worker pool."""
        self._running = False
        for w in self._workers:
            w.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

    async def submit(
        self,
        func: Callable[..., Any],
        *args: Any,
        priority: TaskPriority = TaskPriority.NORMAL,
        deadline: Optional[float] = None,
        timeout: Optional[float] = None,
        task_id: Optional[str] = None,
        **kwargs: Any,
    ) -> asyncio.Future:
        """
        Submit a prioritized callable for asynchronous cluster dispatch.
        Returns a Future that resolves with the execution result.
        """
        import uuid
        tid = task_id or f"ptask_{uuid.uuid4().hex[:8]}"
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        task = PrioritizedTask(
            task_id=tid,
            func=func,
            args=args,
            kwargs=kwargs,
            base_priority=priority,
            deadline=deadline,
            timeout=timeout,
            future=fut,
        )

        await self.queue.push(task)
        return fut

    async def _worker_loop(self, worker_id: int) -> None:
        """Background coroutine consuming prioritized tasks from the queue."""
        while self._running:
            try:
                task = await self.queue.pop()
                if task.cancelled:
                    self.queue.metrics.total_cancelled += 1
                    if task.future and not task.future.done():
                        task.future.cancel()
                    continue

                # Record wait time
                wait_ms = task.age_seconds * 1000.0
                self._total_wait_time_ms += wait_ms
                self._processed_tasks_count += 1
                self.queue.metrics.avg_wait_time_ms = round(
                    self._total_wait_time_ms / max(1, self._processed_tasks_count), 2
                )

                # Execute task via scheduler or direct callable
                try:
                    if hasattr(self.scheduler, "dispatch_task"):
                        result = await self.scheduler.dispatch_task(
                            task.func,
                            *task.args,
                            **task.kwargs,
                        )
                    else:
                        # Fallback for direct testing callable
                        res = task.func(*task.args, **task.kwargs)
                        if asyncio.iscoroutine(res):
                            result = await res
                        else:
                            result = res

                    self.queue.metrics.total_completed += 1
                    if task.future and not task.future.done():
                        task.future.set_result(result)
                except Exception as exc:
                    self.queue.metrics.total_failed += 1
                    if task.future and not task.future.done():
                        task.future.set_exception(exc)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} encountered unexpected error: {e}", exc_info=True)
                await asyncio.sleep(0.1)


