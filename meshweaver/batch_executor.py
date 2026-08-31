"""
MeshWeaver Parallel Batch Executor Module.
Provides distributed MapReduce-style parallel batch computation across
mesh nodes with concurrency throttling and chunking support.
"""

import asyncio
from dataclasses import dataclass, field
import logging
import time
from typing import Any, AsyncGenerator, Callable, Iterable, List, Optional, Sequence, Tuple

from meshweaver.scheduler import RetryPolicy, SchedulingPolicy, TaskScheduler

logger = logging.getLogger("meshweaver.batch_executor")


@dataclass
class BatchMetrics:
    """Telemetry data capturing the performance of a parallel batch execution."""
    total_items: int = 0
    completed_items: int = 0
    failed_items: int = 0
    duration_seconds: float = 0.0

    @property
    def throughput(self) -> float:
        """Calculate processed items per second."""
        if self.duration_seconds <= 0:
            return 0.0
        return round(self.completed_items / self.duration_seconds, 2)


def chunk_iterable(items: Sequence[Any], chunk_size: int = 1) -> List[List[Any]]:
    """Partition a sequence into consecutive chunks of size at most chunk_size."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least 1")
    return [list(items[i : i + chunk_size]) for i in range(0, len(items), chunk_size)]


class ParallelBatchExecutor:
    """
    Distributes batch task execution across mesh peer workers in parallel.
    """

    def __init__(
        self,
        scheduler: TaskScheduler,
        default_concurrency: int = 10,
    ):
        self.scheduler = scheduler
        self.default_concurrency = max(1, default_concurrency)

    async def map(
        self,
        func: Callable[[Any], Any],
        iterable: Sequence[Any],
        chunk_size: int = 1,
        concurrency: Optional[int] = None,
        policy: Optional[SchedulingPolicy] = None,
        retry_policy: Optional[RetryPolicy] = None,
        return_exceptions: bool = False,
    ) -> Tuple[List[Any], BatchMetrics]:
        """
        Apply func to all items in iterable in parallel across the mesh.
        Returns a tuple of (ordered_results, BatchMetrics).
        """
        items = list(iterable)
        if not items:
            return [], BatchMetrics(total_items=0, completed_items=0, failed_items=0, duration_seconds=0.0)

        limit = concurrency or self.default_concurrency
        semaphore = asyncio.Semaphore(limit)
        start_time = time.perf_counter()

        metrics = BatchMetrics(total_items=len(items))

        async def _worker_invoke(idx: int, item: Any) -> Tuple[int, Any]:
            async with semaphore:
                try:
                    res = await self.scheduler.dispatch_task(
                        func,
                        item,
                        policy=policy,
                        retry_policy=retry_policy,
                    )
                    metrics.completed_items += 1
                    return idx, res
                except Exception as exc:
                    metrics.failed_items += 1
                    if return_exceptions:
                        return idx, exc
                    raise

        # If chunking requested
        if chunk_size > 1:
            chunks = chunk_iterable(items, chunk_size=chunk_size)

            def chunk_wrapper(chunk_list: List[Any]) -> List[Any]:
                return [func(x) for x in chunk_list]

            async def _chunk_worker(c_idx: int, chunk_list: List[Any]) -> Tuple[int, List[Any]]:
                async with semaphore:
                    try:
                        res = await self.scheduler.dispatch_task(
                            chunk_wrapper,
                            chunk_list,
                            policy=policy,
                            retry_policy=retry_policy,
                        )
                        metrics.completed_items += len(chunk_list)
                        return c_idx, res
                    except Exception as exc:
                        metrics.failed_items += len(chunk_list)
                        if return_exceptions:
                            return c_idx, exc
                        raise

            tasks = [_chunk_worker(i, c) for i, c in enumerate(chunks)]
            chunk_results = await asyncio.gather(*tasks, return_exceptions=return_exceptions)

            # Flatten ordered chunk results
            ordered_items = []
            for item in sorted(chunk_results, key=lambda x: x[0] if isinstance(x, tuple) else -1):
                if isinstance(item, tuple):
                    if isinstance(item[1], list):
                        ordered_items.extend(item[1])
                    else:
                        ordered_items.append(item[1])
                else:
                    ordered_items.append(item)

            metrics.duration_seconds = round(time.perf_counter() - start_time, 3)
            return ordered_items, metrics

        tasks = [_worker_invoke(i, item) for i, item in enumerate(items)]
        raw_results = await asyncio.gather(*tasks, return_exceptions=return_exceptions)

        # Sort back to preserve original index order
        ordered_results = []
        for res in sorted(raw_results, key=lambda x: x[0] if isinstance(x, tuple) else -1):
            if isinstance(res, tuple):
                ordered_results.append(res[1])
            else:
                ordered_results.append(res)

        metrics.duration_seconds = round(time.perf_counter() - start_time, 3)
        return ordered_results, metrics


