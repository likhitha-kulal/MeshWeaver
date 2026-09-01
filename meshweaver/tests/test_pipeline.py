"""
Unit tests for MeshWeaver TaskPipeline and PipelineStage engine.
"""

import asyncio
import unittest

from meshweaver.pipeline import PipelineStage, TaskPipeline
from meshweaver.scheduler import TaskScheduler


def double_fn(x: int) -> int:
    return x * 2


def add_five(x: int) -> int:
    return x + 5


def sum_all(items: list) -> int:
    return sum(items)


def failing_fn(x: int) -> int:
    raise ValueError("Deliberate failure in pipeline stage")


class TestPipelineUnit(unittest.IsolatedAsyncioTestCase):
    """Unit tests for TaskPipeline."""

    async def asyncSetUp(self):
        self.scheduler = TaskScheduler(local_node_id="local_pipeline_test")
        self.pipeline = TaskPipeline(scheduler=self.scheduler)

    async def test_multi_stage_pipeline_execution(self):
        # Stage 1: [1, 2, 3] -> [2, 4, 6] (Parallel)
        # Stage 2: [2, 4, 6] -> [7, 9, 11] (Parallel)
        # Stage 3: [7, 9, 11] -> 27 (Sequential aggregate)
        self.pipeline.pipe("Double", double_fn, is_parallel=True)
        self.pipeline.pipe("Add 5", add_five, is_parallel=True)
        self.pipeline.add_stage("Sum Aggregator", sum_all, is_parallel=False)

        result, metrics = await self.pipeline.execute([1, 2, 3])

        self.assertEqual(result, 27)
        self.assertEqual(metrics.total_stages, 3)
        self.assertTrue(metrics.is_successful)
        self.assertEqual(len(metrics.stages), 3)
        self.assertEqual(metrics.stages[0].status, "COMPLETED")
        self.assertEqual(metrics.stages[1].status, "COMPLETED")
        self.assertEqual(metrics.stages[2].status, "COMPLETED")
        self.assertEqual(metrics.stages[2].output_count, 1)

    async def test_pipeline_failure_capture(self):
        self.pipeline.pipe("Double", double_fn)
        self.pipeline.pipe("Faulty Stage", failing_fn)
        self.pipeline.pipe("Unreached Stage", add_five)

        with self.assertRaises(ValueError):
            await self.pipeline.execute([1, 2, 3])


if __name__ == "__main__":
    unittest.main()
