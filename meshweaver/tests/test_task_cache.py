"""
Unit tests for TaskCache DHT-backed result memoization.
"""

from unittest.mock import AsyncMock, MagicMock
import unittest

from meshweaver.task_cache import TaskCache


def square_number(x: int) -> int:
    return x * x


class TestTaskCacheKeyGeneration(unittest.TestCase):
    """Test deterministic cache key generation."""

    def test_cache_key_determinism(self):
        k1 = TaskCache.compute_cache_key(square_number, 5)
        k2 = TaskCache.compute_cache_key(square_number, 5)
        self.assertEqual(k1, k2)
        self.assertTrue(k1.startswith(TaskCache.CACHE_KEY_PREFIX))

    def test_distinct_keys_for_different_args(self):
        k1 = TaskCache.compute_cache_key(square_number, 5)
        k2 = TaskCache.compute_cache_key(square_number, 6)
        self.assertNotEqual(k1, k2)

    def test_kwargs_sorting_determinism(self):
        def sample_kw(a=1, b=2):
            return a + b

        k1 = TaskCache.compute_cache_key(sample_kw, a=10, b=20)
        k2 = TaskCache.compute_cache_key(sample_kw, b=20, a=10)
        self.assertEqual(k1, k2)


class TestTaskCacheExecution(unittest.IsolatedAsyncioTestCase):
    """Test cache get, put, and execute_with_cache logic."""

    async def test_cache_miss_executes_and_stores(self):
        mock_dht = MagicMock()
        mock_dht.find_value = AsyncMock(return_value=None)
        mock_dht.store = AsyncMock(return_value=2)

        cache = TaskCache(dht_storage=mock_dht, default_ttl=60.0)

        call_count = 0
        def compute_fn(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        result = await cache.execute_with_cache(compute_fn, 10)
        self.assertEqual(result, 20)
        self.assertEqual(call_count, 1)
        mock_dht.store.assert_awaited_once()

    async def test_cache_hit_bypasses_execution(self):
        mock_dht = MagicMock()
        mock_dht.find_value = AsyncMock(return_value=999)
        mock_dht.store = AsyncMock()

        cache = TaskCache(dht_storage=mock_dht)

        call_count = 0
        def heavy_compute() -> int:
            nonlocal call_count
            call_count += 1
            return 12345

        result = await cache.execute_with_cache(heavy_compute)
        self.assertEqual(result, 999)
        self.assertEqual(call_count, 0)
        mock_dht.store.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
