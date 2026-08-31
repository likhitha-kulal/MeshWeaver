"""
Unit tests for ParallelBatchExecutor and chunk_iterable.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock
import unittest

from meshweaver.batch_executor import BatchMetrics, ParallelBatchExecutor, chunk_iterable
from meshweaver.scheduler import TaskScheduler


def double_val(x: int) -> int:
    return x * 2


class TestBatchExecutor(unittest.IsolatedAsyncioTestCase):
    """Test suite for parallel batch execution and chunking."""

    def test_chunk_iterable(self):
        items = list(range(10))
        chunks = chunk_iterable(items, chunk_size=3)
        self.assertEqual(chunks, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])

        # Invalid chunk size raises
        with self.assertRaises(ValueError):
            chunk_iterable(items, chunk_size=0)

    def test_batch_metrics(self):
        metrics = BatchMetrics(total_items=10, completed_items=8, failed_items=2, duration_seconds=2.0)
        self.assertEqual(metrics.throughput, 4.0)

    async def test_parallel_map_ordered(self):
        scheduler = TaskScheduler(local_node_id="local_node")
        executor = ParallelBatchExecutor(scheduler=scheduler, default_concurrency=4)

        inputs = [1, 2, 3, 4, 5]
        results, metrics = await executor.map(double_val, inputs)

        self.assertEqual(results, [2, 4, 6, 8, 10])
        self.assertEqual(metrics.completed_items, 5)
        self.assertEqual(metrics.failed_items, 0)
        self.assertGreater(metrics.duration_seconds, 0)

    async def test_parallel_map_with_chunking(self):
        scheduler = TaskScheduler(local_node_id="local_node")
        executor = ParallelBatchExecutor(scheduler=scheduler, default_concurrency=2)

        inputs = list(range(12))
        results, metrics = await executor.map(double_val, inputs, chunk_size=4)

        self.assertEqual(results, [x * 2 for x in inputs])
        self.assertEqual(metrics.completed_items, 12)

    async def test_map_unordered_streaming(self):
        scheduler = TaskScheduler(local_node_id="local_node")
        executor = ParallelBatchExecutor(scheduler=scheduler, default_concurrency=3)

        inputs = [10, 20, 30]
        streamed = []
        async for idx, val in executor.map_unordered(double_val, inputs):
            streamed.append((idx, val))

        self.assertEqual(len(streamed), 3)
        sorted_res = sorted(streamed, key=lambda x: x[0])
        self.assertEqual([val for _, val in sorted_res], [20, 40, 60])


if __name__ == "__main__":
    unittest.main()
