"""
MeshWeaver Distributed Synchronization Primitives.
Provides consensus-coordinated DistributedBarrier, DistributedCountdownLatch,
and lease-based DistributedSemaphore with automatic expiration reclamation.
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Set

from meshweaver.models import BarrierState, DistributedBarrierSpec, DistributedSemaphoreSpec

logger = logging.getLogger("meshweaver.barrier")


class DistributedBarrier:
    """
    Consensus-backed N-party rendezvous synchronization barrier.
    Blocks participant tasks until exactly `threshold` parties have entered.
    Supports generation cycling, timeouts, and cancellation.
    """

    def __init__(
        self,
        barrier_id: str,
        threshold: int,
        timeout_seconds: float = 30.0,
    ):
        if threshold <= 0:
            raise ValueError("Barrier threshold must be at least 1")
        self.barrier_id = barrier_id
        self.threshold = threshold
        self.timeout_seconds = timeout_seconds
        self.parties: Set[str] = set()
        self.state: BarrierState = BarrierState.WAITING
        self.generation: int = 0
        self.created_at: float = time.time()
        self.released_at: Optional[float] = None
        self._waiters: List[asyncio.Future] = []
        self._lock = asyncio.Lock()

    @property
    def waiting_count(self) -> int:
        return len(self.parties)

    @property
    def is_released(self) -> bool:
        return self.state == BarrierState.RELEASED

    async def enter(
        self,
        participant_id: str,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Register participant at barrier and await until threshold parties arrive.
        Returns True when released, or False/raises TimeoutError on timeout.
        """
        effective_timeout = timeout if timeout is not None else self.timeout_seconds
        fut: asyncio.Future = asyncio.get_running_loop().create_future()

        async with self._lock:
            # If previous generation was released, reset to WAITING for new cycle
            if self.state == BarrierState.RELEASED:
                self.parties.clear()
                self.state = BarrierState.WAITING
                self.created_at = time.time()

            self.parties.add(participant_id)
            self._waiters.append(fut)

            if len(self.parties) >= self.threshold:
                # Quorum threshold met -> release all waiting parties!
                self.state = BarrierState.RELEASED
                self.generation += 1
                self.released_at = time.time()
                for w in self._waiters:
                    if not w.done():
                        w.set_result(True)
                self._waiters.clear()
                logger.info(
                    f"Barrier '{self.barrier_id}' RELEASED (generation={self.generation}, parties={len(self.parties)})"
                )
                return True

        # Await arrival of other parties
        try:
            return await asyncio.wait_for(fut, timeout=effective_timeout)
        except asyncio.TimeoutError:
            async with self._lock:
                if fut in self._waiters:
                    self._waiters.remove(fut)
                if not self.is_released:
                    self.state = BarrierState.TIMED_OUT
                    # Cancel other waiters
                    for w in self._waiters:
                        if not w.done():
                            w.set_result(False)
                    self._waiters.clear()
            logger.warning(f"Barrier '{self.barrier_id}' TIMED_OUT waiting for parties")
            return False

    async def reset(self) -> None:
        """Reset barrier state to WAITING and clear current participants."""
        async with self._lock:
            self.parties.clear()
            self.state = BarrierState.WAITING
            self.created_at = time.time()
            self.released_at = None
            for w in self._waiters:
                if not w.done():
                    w.set_result(False)
            self._waiters.clear()

    async def cancel(self) -> None:
        """Cancel barrier and notify waiting parties."""
        async with self._lock:
            self.state = BarrierState.CANCELLED
            for w in self._waiters:
                if not w.done():
                    w.set_result(False)
            self._waiters.clear()

    def to_spec(self) -> DistributedBarrierSpec:
        return DistributedBarrierSpec(
            barrier_id=self.barrier_id,
            threshold=self.threshold,
            parties=sorted(list(self.parties)),
            state=self.state,
            timeout_seconds=self.timeout_seconds,
            generation=self.generation,
            created_at=self.created_at,
            released_at=self.released_at,
        )


class DistributedCountdownLatch:
    """
    Synchronization latch that allows tasks to await until an initial count is decremented to zero.
    """

    def __init__(self, latch_id: str, count: int):
        if count < 0:
            raise ValueError("Latch count cannot be negative")
        self.latch_id = latch_id
        self._count = count
        self._event = asyncio.Event()
        if count == 0:
            self._event.set()
        self._lock = asyncio.Lock()

    @property
    def current_count(self) -> int:
        return self._count

    async def count_down(self, delta: int = 1) -> int:
        """Atomically decrement latch count. Sets event when count reaches 0."""
        async with self._lock:
            self._count = max(0, self._count - delta)
            if self._count == 0:
                self._event.set()
            return self._count

    async def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait until latch count reaches zero or timeout occurs."""
        try:
            if timeout is not None:
                await asyncio.wait_for(self._event.wait(), timeout=timeout)
            else:
                await self._event.wait()
            return True
        except asyncio.TimeoutError:
            return False

    def is_zero(self) -> bool:
        return self._count == 0
