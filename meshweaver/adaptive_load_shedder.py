"""
MeshWeaver Adaptive Load-Shedding & Dynamic Backpressure Engine.
Monitors CPU, RAM, and active worker concurrency to calculate composite load watermarks,
dynamically adjusts token bucket replenishment rates, and enforces QoS-aware load shedding.
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional

from meshweaver.models import BackpressureStatus, LoadShedderMetrics, TokenBucketConfig

logger = logging.getLogger("meshweaver.load_shedder")


class AdaptiveLoadShedder:
    """
    Dynamic concurrency and admission control engine.
    Calculates composite load watermarks and sheds lower-priority workloads
    under high cluster pressure to protect consensus and critical tasks.
    """

    def __init__(
        self,
        config: Optional[TokenBucketConfig] = None,
        max_concurrency: int = 16,
        min_concurrency: int = 2,
    ):
        self.config = config or TokenBucketConfig()
        self.max_concurrency = max_concurrency
        self.min_concurrency = min_concurrency
        self._current_concurrency: int = 0
        self._tokens: float = self.config.capacity
        self._last_refill: float = time.time()
        self._cached_cpu: float = 0.0
        self._cached_ram: float = 0.0
        self.total_admitted: int = 0
        self.total_shed: int = 0
        self._lock = asyncio.Lock()

    @property
    def current_concurrency(self) -> int:
        return self._current_concurrency

    def update_resource_telemetry(self, cpu_percent: float, ram_percent: float) -> None:
        """Update recent CPU and RAM utilization measurements."""
        self._cached_cpu = max(0.0, min(100.0, cpu_percent))
        self._cached_ram = max(0.0, min(100.0, ram_percent))

    def calculate_watermark(self) -> float:
        """
        Calculate composite load watermark W in range [0.0, 1.0].
        W = 0.45 * (CPU/100) + 0.35 * (RAM/100) + 0.20 * (concurrency / max_concurrency)
        """
        cpu_norm = self._cached_cpu / 100.0
        ram_norm = self._cached_ram / 100.0
        concurrency_norm = min(1.0, self._current_concurrency / max(1, self.max_concurrency))
        watermark = (0.45 * cpu_norm) + (0.35 * ram_norm) + (0.20 * concurrency_norm)
        return min(1.0, max(0.0, watermark))

    def get_status(self) -> BackpressureStatus:
        """Determine backpressure status tier based on watermark."""
        w = self.calculate_watermark()
        if w >= 0.85:
            return BackpressureStatus.CRITICAL
        elif w >= 0.75:
            return BackpressureStatus.HIGH
        elif w >= 0.60:
            return BackpressureStatus.MODERATE
        return BackpressureStatus.NORMAL

    def get_effective_refill_rate(self) -> float:
        """Dynamically throttle token refill rate based on backpressure."""
        status = self.get_status()
        base = self.config.refill_rate
        min_rate = self.config.min_refill_rate

        if status == BackpressureStatus.CRITICAL:
            return min_rate
        elif status == BackpressureStatus.HIGH:
            return max(min_rate, base * 0.40)
        elif status == BackpressureStatus.MODERATE:
            return max(min_rate, base * 0.75)
        return base

    def _refill_tokens(self, now: Optional[float] = None) -> None:
        """Refill token bucket according to elapsed time and effective rate."""
        current_ts = now if now is not None else time.time()
        elapsed = max(0.0, current_ts - self._last_refill)
        rate = self.get_effective_refill_rate()
        self._tokens = min(self.config.capacity, self._tokens + (elapsed * rate))
        self._last_refill = current_ts

    async def try_acquire(self, priority: int = 2, cost: float = 1.0) -> bool:
        """
        Attempt to admit a task based on token availability and QoS priority tier.
        Priority tiers: 0=CRITICAL, 1=HIGH, 2=NORMAL, 3=LOW, 4=BACKGROUND.
        Returns True if admitted, False if shed.
        """
        now = time.time()
        async with self._lock:
            self._refill_tokens(now)
            status = self.get_status()

            # QoS shedding policies under backpressure
            if status == BackpressureStatus.CRITICAL:
                # In CRITICAL state, reject everything below HIGH priority (priority > 1)
                if priority > 1:
                    self.total_shed += 1
                    logger.warning(
                        f"LoadShedder CRITICAL backpressure: shed priority {priority} task"
                    )
                    return False
            elif status == BackpressureStatus.HIGH:
                # In HIGH state, reject BACKGROUND priority (priority >= 4)
                if priority >= 4:
                    self.total_shed += 1
                    logger.debug(
                        f"LoadShedder HIGH backpressure: shed priority {priority} task"
                    )
                    return False

            # Check concurrency ceiling
            if self._current_concurrency >= self.max_concurrency:
                # Only CRITICAL priority (0) allowed to exceed ceiling slightly
                if priority > 0:
                    self.total_shed += 1
                    return False

            # Check token availability
            if self._tokens >= cost:
                self._tokens -= cost
                self._current_concurrency += 1
                self.total_admitted += 1
                return True

            self.total_shed += 1
            return False

    async def release(self) -> None:
        """Release concurrency permit upon task completion."""
        async with self._lock:
            self._current_concurrency = max(0, self._current_concurrency - 1)

    def get_metrics(self) -> LoadShedderMetrics:
        """Return operational telemetry snapshot."""
        w = self.calculate_watermark()
        status = self.get_status()
        rate = self.get_effective_refill_rate()
        return LoadShedderMetrics(
            cpu_percent=self._cached_cpu,
            ram_percent=self._cached_ram,
            composite_watermark=round(w, 4),
            status=status,
            total_admitted=self.total_admitted,
            total_shed=self.total_shed,
            effective_rate=round(rate, 2),
            current_concurrency=self._current_concurrency,
            max_concurrency=self.max_concurrency,
        )
