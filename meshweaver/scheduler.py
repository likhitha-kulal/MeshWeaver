"""
MeshWeaver Task Scheduler Module.
Provides intelligent load-balanced task dispatching, retry policies,
and failover mechanisms across active mesh peer nodes.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
import inspect
import logging
import random
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from meshweaver.circuit_breaker import CircuitBreakerConfig, CircuitBreakerRegistry, CircuitState
from meshweaver.networking import TCPTaskClient
from meshweaver.task_serializer import RemoteExecutionError, TaskSerializer


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
        circuit_breakers: Optional[CircuitBreakerRegistry] = None,
    ):
        self.local_node_id = local_node_id
        self.gossip_manager = gossip_manager
        self.load_scorer = load_scorer or LoadScorer()
        self.default_policy = default_policy
        self.retry_policy = default_retry_policy or RetryPolicy()
        self.circuit_breakers = circuit_breakers or CircuitBreakerRegistry()
        self._round_robin_index = 0
        self._active_tasks: Dict[str, int] = {}  # node_id -> active task count

    def get_active_candidates(
        self,
        exclude_node_ids: Optional[Set[str]] = None,
    ) -> List[WorkerCandidate]:
        """
        Retrieve alive peer candidates from GossipManager and compute their composite load scores.
        Filters out nodes whose circuit breaker is currently OPEN.
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
            if not self.circuit_breakers.is_node_available(node_id):
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

    def _select_round_robin(self, candidates: List[WorkerCandidate]) -> Optional[WorkerCandidate]:
        """Select candidates in circular order across invocations."""
        if not candidates:
            return None
        selected = candidates[self._round_robin_index % len(candidates)]
        self._round_robin_index = (self._round_robin_index + 1) % len(candidates)
        return selected

    def _select_power_of_two(self, candidates: List[WorkerCandidate]) -> Optional[WorkerCandidate]:
        """
        Sample 2 candidates at random and select the one with the lowest score.
        Mitigates herd effects in large clusters.
        """
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        sample = random.sample(candidates, 2)
        return min(sample, key=lambda c: c.score)

    def select_worker(
        self,
        policy: Optional[SchedulingPolicy] = None,
        exclude_node_ids: Optional[Set[str]] = None,
    ) -> Optional[WorkerCandidate]:
        """
        Select an optimal worker node according to the specified policy,
        excluding any specified node IDs.
        """
        chosen_policy = policy or self.default_policy
        candidates = self.get_active_candidates(exclude_node_ids=exclude_node_ids)
        if not candidates:
            return None

        if chosen_policy == SchedulingPolicy.LEAST_LOADED:
            return self._select_least_loaded(candidates)
        elif chosen_policy == SchedulingPolicy.ROUND_ROBIN:
            return self._select_round_robin(candidates)
        elif chosen_policy == SchedulingPolicy.POWER_OF_TWO_RANDOM:
            return self._select_power_of_two(candidates)
        elif chosen_policy == SchedulingPolicy.LOCAL_FIRST:
            # Check if least loaded remote is significantly better or fallback
            return self._select_least_loaded(candidates)
        else:
            return self._select_least_loaded(candidates)

    async def _execute_local(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute callable locally on this node as fallback."""
        if inspect.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return func(*args, **kwargs)

    async def dispatch_task(
        self,
        func: Callable,
        *args: Any,
        policy: Optional[SchedulingPolicy] = None,
        retry_policy: Optional[RetryPolicy] = None,
        fallback_local: bool = True,
        **kwargs: Any,
    ) -> Any:
        """
        Dispatch task to an optimal remote worker with automatic failover and retry logic.
        """
        retries = retry_policy or self.retry_policy
        excluded_nodes: Set[str] = set()
        last_exception: Optional[Exception] = None

        payload_bytes = TaskSerializer.serialize(func, *args, **kwargs)

        for attempt in range(1, retries.max_retries + 1):
            worker = self.select_worker(policy=policy, exclude_node_ids=excluded_nodes)

            if not worker:
                logger.warning(
                    f"No eligible remote workers found for task {func.__name__} (attempt {attempt}/{retries.max_retries})"
                )
                if fallback_local:
                    logger.info(f"Falling back to local execution for task {func.__name__}")
                    return await self._execute_local(func, *args, **kwargs)
                raise RuntimeError(f"No available compute workers in mesh network for {func.__name__}")

            logger.info(
                f"Dispatching task {func.__name__} to worker {worker.node_id[:8]}... "
                f"({worker.host}:{worker.tcp_port}, score={worker.score}) [attempt {attempt}/{retries.max_retries}]"
            )

            # Track in-flight task for load scoring
            self._active_tasks[worker.node_id] = self._active_tasks.get(worker.node_id, 0) + 1
            start_time = time.perf_counter()

            try:
                task_result = await TCPTaskClient.send_task(
                    worker.host,
                    worker.tcp_port,
                    payload_bytes,
                    timeout=retries.timeout_per_attempt,
                )
                duration = time.perf_counter() - start_time
                logger.info(
                    f"Task {func.__name__} completed on {worker.node_id[:8]}... in {duration:.3f}s"
                )
                self.circuit_breakers.record_node_success(worker.node_id)
                return TaskSerializer.unpack_result(task_result)

            except RemoteExecutionError:
                # Execution failed inside worker user code (logic error/exception), do not trip circuit breaker
                self.circuit_breakers.record_node_success(worker.node_id)
                raise

            except Exception as e:
                duration = time.perf_counter() - start_time
                logger.warning(
                    f"Remote execution failed on worker {worker.node_id[:8]}... after {duration:.3f}s: {e}"
                )
                self.circuit_breakers.record_node_failure(worker.node_id, e)
                last_exception = e
                if retries.exclude_failed_nodes:
                    excluded_nodes.add(worker.node_id)

                if attempt < retries.max_retries:
                    delay = retries.backoff_factor * (2 ** (attempt - 1))
                    logger.info(f"Retrying task {func.__name__} in {delay:.2f}s...")
                    await asyncio.sleep(delay)

            finally:
                if worker.node_id in self._active_tasks:
                    self._active_tasks[worker.node_id] = max(0, self._active_tasks[worker.node_id] - 1)

        # All retries exhausted
        if fallback_local:
            logger.warning(
                f"All remote dispatch attempts exhausted for {func.__name__}. Falling back to local execution."
            )
            return await self._execute_local(func, *args, **kwargs)

        raise RuntimeError(
            f"Failed to execute task {func.__name__} after {retries.max_retries} attempts: {last_exception}"
        )

    def get_stats(self) -> Dict[str, Any]:
        """Return snapshot of active task distribution and circuit breaker health across workers."""
        return {
            "default_policy": self.default_policy.value,
            "active_tasks_by_node": dict(self._active_tasks),
            "total_active_tasks": sum(self._active_tasks.values()),
            "active_candidates_count": len(self.get_active_candidates()),
            "tripped_circuits": self.circuit_breakers.get_tripped_nodes(),
        }










