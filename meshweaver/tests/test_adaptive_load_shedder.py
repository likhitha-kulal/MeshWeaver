"""
Unit tests for AdaptiveLoadShedder, watermark calculations, and QoS backpressure shedding.
"""

import asyncio
import unittest

from meshweaver.adaptive_load_shedder import AdaptiveLoadShedder
from meshweaver.models import BackpressureStatus, TokenBucketConfig


class TestAdaptiveLoadShedder(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = TokenBucketConfig(
            capacity=10.0,
            refill_rate=20.0,
            min_refill_rate=2.0,
            backpressure_threshold=0.85,
        )
        self.shedder = AdaptiveLoadShedder(
            config=self.config,
            max_concurrency=4,
            min_concurrency=1,
        )

    async def test_watermark_calculation_and_status_transitions(self):
        # 1. Low load -> NORMAL
        self.shedder.update_resource_telemetry(cpu_percent=20.0, ram_percent=30.0)
        w1 = self.shedder.calculate_watermark()
        self.assertTrue(w1 < 0.60)
        self.assertEqual(self.shedder.get_status(), BackpressureStatus.NORMAL)
        self.assertEqual(self.shedder.get_effective_refill_rate(), 20.0)

        # 2. Medium load -> MODERATE (w in [0.60, 0.75))
        self.shedder.update_resource_telemetry(cpu_percent=80.0, ram_percent=75.0)
        w2 = self.shedder.calculate_watermark()
        self.assertTrue(0.60 <= w2 < 0.75)
        self.assertEqual(self.shedder.get_status(), BackpressureStatus.MODERATE)
        self.assertEqual(self.shedder.get_effective_refill_rate(), 15.0)  # 20 * 0.75

        # 3. High load -> HIGH (w in [0.75, 0.85))
        self.shedder.update_resource_telemetry(cpu_percent=96.0, ram_percent=96.0)
        w3 = self.shedder.calculate_watermark()
        self.assertTrue(0.75 <= w3 < 0.85)
        self.assertEqual(self.shedder.get_status(), BackpressureStatus.HIGH)
        self.assertEqual(self.shedder.get_effective_refill_rate(), 8.0)  # 20 * 0.40

        # 4. Severe load -> CRITICAL (w >= 0.85)
        self.shedder.update_resource_telemetry(cpu_percent=98.0, ram_percent=98.0)
        # Add concurrency to push over 0.85
        self.shedder._current_concurrency = 3
        w4 = self.shedder.calculate_watermark()
        self.assertTrue(w4 >= 0.85)
        self.assertEqual(self.shedder.get_status(), BackpressureStatus.CRITICAL)
        self.assertEqual(self.shedder.get_effective_refill_rate(), 2.0)  # min_refill_rate
        self.shedder._current_concurrency = 0

    async def test_qos_task_admission_and_shedding(self):
        # Under normal conditions, all priorities admitted
        self.shedder.update_resource_telemetry(cpu_percent=10.0, ram_percent=10.0)
        ok_crit = await self.shedder.try_acquire(priority=0)
        ok_normal = await self.shedder.try_acquire(priority=2)
        ok_bg = await self.shedder.try_acquire(priority=4)

        self.assertTrue(ok_crit)
        self.assertTrue(ok_normal)
        self.assertTrue(ok_bg)

        # Release permits
        await self.shedder.release()
        await self.shedder.release()
        await self.shedder.release()

        # Under HIGH pressure -> BACKGROUND (4) should be shed
        self.shedder.update_resource_telemetry(cpu_percent=96.0, ram_percent=96.0)
        shed_bg = await self.shedder.try_acquire(priority=4)
        admit_high = await self.shedder.try_acquire(priority=1)

        self.assertFalse(shed_bg)
        self.assertTrue(admit_high)
        await self.shedder.release()

        # Under CRITICAL pressure -> NORMAL (2) shed, CRITICAL (0) admitted
        self.shedder.update_resource_telemetry(cpu_percent=98.0, ram_percent=98.0)
        self.shedder._current_concurrency = 2
        shed_normal = await self.shedder.try_acquire(priority=2)
        admit_crit = await self.shedder.try_acquire(priority=0)

        self.assertFalse(shed_normal)
        self.assertTrue(admit_crit)
        await self.shedder.release()

    async def test_concurrency_ceiling_and_tuning(self):
        self.shedder.update_resource_telemetry(cpu_percent=10.0, ram_percent=10.0)
        # Fill up to max_concurrency (4)
        p1 = await self.shedder.try_acquire(priority=1)
        p2 = await self.shedder.try_acquire(priority=1)
        p3 = await self.shedder.try_acquire(priority=1)
        p4 = await self.shedder.try_acquire(priority=1)
        self.assertTrue(p1 and p2 and p3 and p4)
        self.assertEqual(self.shedder.current_concurrency, 4)

        # 5th task with NORMAL priority should be rejected due to concurrency limit
        p5 = await self.shedder.try_acquire(priority=2)
        self.assertFalse(p5)

        # Release 1
        await self.shedder.release()
        self.assertEqual(self.shedder.current_concurrency, 3)

        # Auto-tuning concurrency
        self.shedder.update_resource_telemetry(cpu_percent=92.0, ram_percent=50.0)
        new_limit = self.shedder.tune_concurrency_limits(target_cpu_percent=75.0)
        self.assertEqual(new_limit, 3)  # Decreased from 4 to 3

        metrics = self.shedder.get_metrics()
        self.assertEqual(metrics.max_concurrency, 3)
        self.assertTrue(metrics.total_admitted >= 4)
        self.assertTrue(metrics.total_shed >= 1)


if __name__ == "__main__":
    unittest.main()
