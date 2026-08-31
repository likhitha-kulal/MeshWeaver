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


if __name__ == "__main__":
    unittest.main()
