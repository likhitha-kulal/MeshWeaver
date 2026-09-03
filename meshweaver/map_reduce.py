"""
MeshWeaver Distributed MapReduce & Aggregation Engine.
Provides distributed Map -> Shuffle/Partition -> Reduce compute pipelines and tree-based parallel reduction.
"""

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
import logging
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, TypeVar, Union

from meshweaver.scheduler import SchedulingPolicy, TaskScheduler

logger = logging.getLogger("meshweaver.map_reduce")

K = TypeVar("K")
V = TypeVar("V")
R = TypeVar("R")


@dataclass
class MapReduceMetrics:
    """Telemetry captured during distributed MapReduce execution."""
    total_input_items: int = 0
    total_intermediate_pairs: int = 0
    total_partitions: int = 0
    map_duration_seconds: float = 0.0
    shuffle_duration_seconds: float = 0.0
    reduce_duration_seconds: float = 0.0
    total_duration_seconds: float = 0.0
    workers_utilized: int = 0
    throughput_items_per_sec: float = 0.0
    tripped_nodes: List[str] = field(default_factory=list)


def default_hash_partitioner(key: Any, num_partitions: int) -> int:
    """Hash-based partitioner for routing intermediate keys to reducers."""
    return hash(key) % num_partitions


class DistributedMapReduce:
    """
    Coordinates distributed MapReduce computations across peer worker nodes in the mesh.
    """

    def __init__(
        self,
        scheduler: TaskScheduler,
        default_concurrency: int = 8,
        default_chunk_size: int = 1,
    ):
        self.scheduler = scheduler
        self.default_concurrency = default_concurrency
        self.default_chunk_size = default_chunk_size

    @staticmethod
    def _chunk_iterable(data: Iterable[Any], chunk_size: int) -> List[List[Any]]:
        items = list(data)
        if not items:
            return []
        if chunk_size <= 1:
            return [[item] for item in items]
        return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]

    async def execute_map_reduce(
        self,
        map_fn: Callable[[Any], List[Tuple[Any, Any]]],
        reduce_fn: Callable[[Any, List[Any]], Any],
        data: Iterable[Any],
        chunk_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        policy: SchedulingPolicy = SchedulingPolicy.LEAST_LOADED,
    ) -> Tuple[Dict[Any, Any], MapReduceMetrics]:
        """
        Execute a full distributed MapReduce workflow:
        1. Map Phase: maps input items to intermediate (key, value) pairs across workers.
        2. Shuffle/Partition Phase: groups values by key locally.
        3. Reduce Phase: dispatches reduce tasks across workers for each key group.
        """
        total_start = time.perf_counter()
        items = list(data)
        c_size = chunk_size or self.default_chunk_size
        max_conc = concurrency or self.default_concurrency
        metrics = MapReduceMetrics(total_input_items=len(items))

        if not items:
            metrics.total_duration_seconds = time.perf_counter() - total_start
            return {}, metrics

        # ----------------------------------------------------
        # Phase 1: Distributed Map Phase
        # ----------------------------------------------------
        map_start = time.perf_counter()
        chunks = self._chunk_iterable(items, c_size)
        semaphore = asyncio.Semaphore(max_conc)
        unique_workers = set()

        async def _run_map_chunk(chunk: List[Any]) -> List[Tuple[Any, Any]]:
            async with semaphore:
                # Helper wrapper to map over chunk items
                def _map_chunk_wrapper(elements: List[Any]) -> List[Tuple[Any, Any]]:
                    pairs: List[Tuple[Any, Any]] = []
                    for el in elements:
                        res = map_fn(el)
                        if isinstance(res, list):
                            pairs.extend(res)
                        elif res is not None:
                            pairs.append(res)
                    return pairs

                res = await self.scheduler.dispatch_task(
                    _map_chunk_wrapper,
                    chunk,
                    policy=policy,
                )
                return res

        map_tasks = [_run_map_chunk(chunk) for chunk in chunks]
        map_results = await asyncio.gather(*map_tasks)
        metrics.map_duration_seconds = time.perf_counter() - map_start

        # ----------------------------------------------------
        # Phase 2: Shuffle & Partitioning Phase
        # ----------------------------------------------------
        shuffle_start = time.perf_counter()
        grouped_intermediates: Dict[Any, List[Any]] = defaultdict(list)
        total_pairs = 0

        for chunk_pairs in map_results:
            if not chunk_pairs:
                continue
            for k, v in chunk_pairs:
                grouped_intermediates[k].append(v)
                total_pairs += 1

        metrics.total_intermediate_pairs = total_pairs
        metrics.total_partitions = len(grouped_intermediates)
        metrics.shuffle_duration_seconds = time.perf_counter() - shuffle_start

        if not grouped_intermediates:
            metrics.total_duration_seconds = time.perf_counter() - total_start
            return {}, metrics

        # ----------------------------------------------------
        # Phase 3: Distributed Reduce Phase
        # ----------------------------------------------------
        reduce_start = time.perf_counter()

        async def _run_reduce_key(key: Any, values: List[Any]) -> Tuple[Any, Any]:
            async with semaphore:
                # Helper wrapper for reduce execution
                def _reduce_wrapper(k: Any, vals: List[Any]) -> Tuple[Any, Any]:
                    reduced_val = reduce_fn(k, vals)
                    return (k, reduced_val)

                return await self.scheduler.dispatch_task(
                    _reduce_wrapper,
                    key,
                    values,
                    policy=policy,
                )

        reduce_tasks = [
            _run_reduce_key(k, v_list)
            for k, v_list in grouped_intermediates.items()
        ]
        reduced_pairs = await asyncio.gather(*reduce_tasks)
        metrics.reduce_duration_seconds = time.perf_counter() - reduce_start

        final_output: Dict[Any, Any] = dict(reduced_pairs)

        total_elapsed = time.perf_counter() - total_start
        metrics.total_duration_seconds = total_elapsed
        if total_elapsed > 0:
            metrics.throughput_items_per_sec = len(items) / total_elapsed
        if hasattr(self.scheduler, "circuit_breakers"):
            metrics.tripped_nodes = self.scheduler.circuit_breakers.get_tripped_nodes()

        return final_output, metrics

    async def tree_reduce(
        self,
        reduce_fn: Callable[[Any, Any], Any],
        data: Iterable[Any],
        initial_value: Optional[Any] = None,
        branching_factor: int = 2,
        policy: SchedulingPolicy = SchedulingPolicy.LEAST_LOADED,
    ) -> Any:
        """
        Hierarchical parallel tree reduction for associative operators (e.g. sum, min, max).
        Reduces items level-by-level across distributed workers in O(log_b N) parallel steps.
        """
        items = list(data)
        if not items:
            return initial_value

        if len(items) == 1 and initial_value is None:
            return items[0]

        current_level = items

        while len(current_level) > 1:
            next_level = []
            chunk_pairs = []

            for i in range(0, len(current_level), branching_factor):
                group = current_level[i : i + branching_factor]
                if len(group) == 1:
                    next_level.append(group[0])
                else:
                    chunk_pairs.append(group)

            if chunk_pairs:
                def _reduce_group_wrapper(elements: List[Any]) -> Any:
                    acc = elements[0]
                    for el in elements[1:]:
                        acc = reduce_fn(acc, el)
                    return acc

                tasks = [
                    self.scheduler.dispatch_task(
                        _reduce_group_wrapper,
                        grp,
                        policy=policy,
                    )
                    for grp in chunk_pairs
                ]
                reduced_chunks = await asyncio.gather(*tasks)
                next_level.extend(reduced_chunks)

            current_level = next_level

        result = current_level[0]
        if initial_value is not None:
            result = reduce_fn(initial_value, result)
        return result
