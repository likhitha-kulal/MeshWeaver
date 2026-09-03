"""
Unit tests for MeshWeaver DistributedMapReduce & tree_reduce engine.
"""

import asyncio
import unittest
from typing import List, Tuple

from meshweaver.map_reduce import DistributedMapReduce, MapReduceMetrics, default_hash_partitioner
from meshweaver.scheduler import SchedulingPolicy, TaskScheduler


def word_mapper(sentence: str) -> List[Tuple[str, int]]:
    return [(w.lower(), 1) for w in sentence.split()]


def word_reducer(key: str, values: List[int]) -> int:
    return sum(values)


def square_mapper(x: int) -> List[Tuple[str, int]]:
    parity = "even" if x % 2 == 0 else "odd"
    return [(parity, x * x)]


def list_sum_reducer(parity: str, squares: List[int]) -> int:
    return sum(squares)


def add_reducer(a: int, b: int) -> int:
    return a + b


class TestMapReduceUnit(unittest.IsolatedAsyncioTestCase):
    """Unit tests for DistributedMapReduce engine using local fallback scheduler."""

    async def asyncSetUp(self):
        self.scheduler = TaskScheduler(local_node_id="local_test_node")
        self.mr = DistributedMapReduce(scheduler=self.scheduler, default_concurrency=4)

    async def test_word_count_map_reduce(self):
        docs = [
            "hello world",
            "hello mesh",
            "world of mesh compute",
        ]
        results, metrics = await self.mr.execute_map_reduce(
            map_fn=word_mapper,
            reduce_fn=word_reducer,
            data=docs,
            chunk_size=1,
        )

        self.assertEqual(results["hello"], 2)
        self.assertEqual(results["world"], 2)
        self.assertEqual(results["mesh"], 2)
        self.assertEqual(results["compute"], 1)
        self.assertEqual(results["of"], 1)

        self.assertEqual(metrics.total_input_items, 3)
        self.assertEqual(metrics.total_intermediate_pairs, 8)
        self.assertEqual(metrics.total_partitions, 5)
        self.assertGreater(metrics.total_duration_seconds, 0)

    async def test_empty_dataset_map_reduce(self):
        results, metrics = await self.mr.execute_map_reduce(
            map_fn=word_mapper,
            reduce_fn=word_reducer,
            data=[],
        )
        self.assertEqual(results, {})
        self.assertEqual(metrics.total_input_items, 0)
        self.assertEqual(metrics.total_intermediate_pairs, 0)

    async def test_parity_grouping_map_reduce(self):
        numbers = [1, 2, 3, 4, 5, 6]
        # Even squares: 4 + 16 + 36 = 56
        # Odd squares: 1 + 9 + 25 = 35
        results, metrics = await self.mr.execute_map_reduce(
            map_fn=square_mapper,
            reduce_fn=list_sum_reducer,
            data=numbers,
            chunk_size=2,
        )
        self.assertEqual(results["even"], 56)
        self.assertEqual(results["odd"], 35)
        self.assertEqual(metrics.total_input_items, 6)

    async def test_tree_reduce_hierarchical_sum(self):
        numbers = list(range(1, 101))
        expected_sum = sum(numbers)  # 5050

        res = await self.mr.tree_reduce(
            reduce_fn=add_reducer,
            data=numbers,
            branching_factor=4,
        )
        self.assertEqual(res, expected_sum)

    async def test_tree_reduce_edge_cases(self):
        # Empty list with initial value
        res_empty = await self.mr.tree_reduce(add_reducer, [], initial_value=10)
        self.assertEqual(res_empty, 10)

        # Single element
        res_single = await self.mr.tree_reduce(add_reducer, [42])
        self.assertEqual(res_single, 42)

    def test_default_hash_partitioner(self):
        p1 = default_hash_partitioner("apple", 4)
        p2 = default_hash_partitioner("apple", 4)
        self.assertEqual(p1, p2)
        self.assertTrue(0 <= p1 < 4)

    async def test_map_reduce_tripped_nodes_telemetry(self):
        self.scheduler.circuit_breakers.get_or_create("tripped-mr-worker")
        self.scheduler.circuit_breakers.record_node_failure("tripped-mr-worker")
        self.scheduler.circuit_breakers.record_node_failure("tripped-mr-worker")
        self.scheduler.circuit_breakers.record_node_failure("tripped-mr-worker")

        docs = ["hello world", "hello mesh"]
        results, metrics = await self.mr.execute_map_reduce(
            map_fn=word_mapper,
            reduce_fn=word_reducer,
            data=docs,
        )
        self.assertEqual(results["hello"], 2)
        self.assertIn("tripped-mr-worker", metrics.tripped_nodes)


if __name__ == "__main__":
    unittest.main()
