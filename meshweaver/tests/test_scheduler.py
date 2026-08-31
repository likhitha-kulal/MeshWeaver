"""
Unit tests for TaskScheduler, LoadScorer, and worker selection policies.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import unittest

from meshweaver.scheduler import (
    LoadScorer,
    RetryPolicy,
    SchedulingPolicy,
    TaskScheduler,
    WorkerCandidate,
)


class TestLoadScorer(unittest.TestCase):
    """Test suite for LoadScorer composite load calculations."""

    def setUp(self):
        self.scorer = LoadScorer(cpu_weight=0.6, ram_weight=0.4, pending_task_weight=5.0)

    def test_composite_score_calculation(self):
        # 0.6 * 50 + 0.4 * 30 + 5.0 * 2 = 30 + 12 + 10 = 52.0
        score = self.scorer.calculate_score(cpu_percent=50.0, ram_percent=30.0, pending_tasks=2)
        self.assertEqual(score, 52.0)

    def test_lower_score_for_idle_node(self):
        idle_score = self.scorer.calculate_score(cpu_percent=10.0, ram_percent=15.0)
        busy_score = self.scorer.calculate_score(cpu_percent=80.0, ram_percent=70.0)
        self.assertLess(idle_score, busy_score)

    def test_overload_threshold_detection(self):
        self.assertFalse(self.scorer.is_overloaded(50.0, 50.0))
        self.assertTrue(self.scorer.is_overloaded(95.0, 50.0))
        self.assertTrue(self.scorer.is_overloaded(50.0, 96.0))


class TestWorkerSelection(unittest.TestCase):
    """Test suite for scheduling algorithms."""

    def setUp(self):
        self.scheduler = TaskScheduler(local_node_id="local_node_123")
        self.candidates = [
            WorkerCandidate("node_a", "127.0.0.1", 9001, cpu_percent=80.0, ram_percent=60.0, score=72.0),
            WorkerCandidate("node_b", "127.0.0.1", 9002, cpu_percent=20.0, ram_percent=30.0, score=24.0),
            WorkerCandidate("node_c", "127.0.0.1", 9003, cpu_percent=50.0, ram_percent=40.0, score=46.0),
        ]

    def test_least_loaded_selection(self):
        selected = self.scheduler._select_least_loaded(self.candidates)
        self.assertIsNotNone(selected)
        self.assertEqual(selected.node_id, "node_b")
        self.assertEqual(selected.score, 24.0)

    def test_round_robin_selection_order(self):
        s1 = self.scheduler._select_round_robin(self.candidates)
        s2 = self.scheduler._select_round_robin(self.candidates)
        s3 = self.scheduler._select_round_robin(self.candidates)
        s4 = self.scheduler._select_round_robin(self.candidates)

        self.assertEqual([s1.node_id, s2.node_id, s3.node_id, s4.node_id], ["node_a", "node_b", "node_c", "node_a"])

    def test_power_of_two_selection(self):
        selected = self.scheduler._select_power_of_two(self.candidates)
        self.assertIsNotNone(selected)
        # Power of 2 between random 2 will never pick the worst (node_a) if comparing against a better one
        self.assertIn(selected.node_id, ["node_a", "node_b", "node_c"])


class TestRetryPolicyAndFailover(unittest.IsolatedAsyncioTestCase):
    """Test suite for retry policy, failure recovery, and local fallback."""

    async def test_local_fallback_when_no_workers(self):
        scheduler = TaskScheduler(local_node_id="local_node")
        # No gossip manager, no candidates -> should execute locally
        def sample_multiply(x: int, y: int) -> int:
            return x * y

        result = await scheduler.dispatch_task(sample_multiply, 6, 7, fallback_local=True)
        self.assertEqual(result, 42)

    async def test_no_workers_raises_when_fallback_disabled(self):
        scheduler = TaskScheduler(local_node_id="local_node")
        with self.assertRaises(RuntimeError):
            await scheduler.dispatch_task(lambda: 10, fallback_local=False)

    @patch("meshweaver.networking.TCPTaskClient.send_task", new_callable=AsyncMock)
    async def test_failover_excludes_failed_worker_and_retries_next_worker(self, mock_send_task):
        mock_gossip = MagicMock()
        peer_a = MagicMock(host="127.0.0.1", tcp_port=9001, cpu_percent=10.0, ram_percent=10.0, is_alive=True)
        peer_b = MagicMock(host="127.0.0.1", tcp_port=9002, cpu_percent=20.0, ram_percent=20.0, is_alive=True)
        mock_gossip.get_all_peers.return_value = {"node_a": peer_a, "node_b": peer_b}

        scheduler = TaskScheduler(
            local_node_id="local_node",
            gossip_manager=mock_gossip,
            default_retry_policy=RetryPolicy(max_retries=2, backoff_factor=0.01, exclude_failed_nodes=True),
        )

        import cloudpickle
        from meshweaver.models import TaskResult
        # First attempt on node_a fails with ConnectionRefusedError, second on node_b succeeds
        success_result = TaskResult(
            task_id="t1",
            success=True,
            result_bytes=cloudpickle.dumps("recovered_value"),
        )
        mock_send_task.side_effect = [ConnectionRefusedError("Node down"), success_result]

        res = await scheduler.dispatch_task(lambda: "test", fallback_local=False)
        self.assertEqual(res, "recovered_value")
        self.assertEqual(mock_send_task.call_count, 2)






if __name__ == "__main__":
    unittest.main()
