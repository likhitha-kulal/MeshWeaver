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
