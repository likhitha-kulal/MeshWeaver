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


class DistributedSemaphore:
    """
    Consensus-backed distributed counting semaphore.
    Enforces concurrency limits with lease TTL timeouts to prevent orphaned permit leaks.
    """

    def __init__(
        self,
        semaphore_id: str,
        total_permits: int,
        default_ttl_seconds: float = 30.0,
    ):
        if total_permits <= 0:
            raise ValueError("Total permits must be at least 1")
        self.semaphore_id = semaphore_id
        self.total_permits = total_permits
        self.default_ttl_seconds = default_ttl_seconds
        self.holders: Dict[str, float] = {}  # holder_id -> lease_expiry_timestamp
        self._wait_queue: List[Tuple[str, float, asyncio.Future]] = []
        self._lock = asyncio.Lock()

    @property
    def available_permits(self) -> int:
        now = time.time()
        active_count = sum(1 for exp in self.holders.values() if exp > now)
        return max(0, self.total_permits - active_count)

    @property
    def active_permits(self) -> int:
        now = time.time()
        return sum(1 for exp in self.holders.values() if exp > now)

    def cleanup_expired_leases(self, now: Optional[float] = None) -> int:
        """Reclaim permits whose lease TTL has expired."""
        current_ts = now if now is not None else time.time()
        expired = [h for h, exp in self.holders.items() if exp <= current_ts]
        for h in expired:
            del self.holders[h]
        return len(expired)

    async def acquire(
        self,
        holder_id: str,
        ttl_seconds: Optional[float] = None,
        timeout: float = 10.0,
    ) -> bool:
        """
        Acquire a semaphore permit for holder_id with lease TTL.
        Returns True if permit granted, or False on timeout.
        """
        lease_ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl_seconds
        now = time.time()

        async with self._lock:
            self.cleanup_expired_leases(now)

            # Check if holder already owns a permit -> renew lease
            if holder_id in self.holders:
                self.holders[holder_id] = now + lease_ttl
                return True

            if len(self.holders) < self.total_permits:
                self.holders[holder_id] = now + lease_ttl
                logger.debug(
                    f"Permit granted on '{self.semaphore_id}' to {holder_id} (active={len(self.holders)}/{self.total_permits})"
                )
                return True

            # Must wait in queue
            loop = asyncio.get_running_loop()
            fut: asyncio.Future = loop.create_future()
            self._wait_queue.append((holder_id, lease_ttl, fut))

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            async with self._lock:
                self._wait_queue = [item for item in self._wait_queue if item[2] != fut]
            return False

    async def release(self, holder_id: str) -> bool:
        """
        Release permit held by holder_id and immediately notify next waiter in FIFO queue.
        """
        now = time.time()
        async with self._lock:
            self.cleanup_expired_leases(now)
            if holder_id not in self.holders:
                return False

            del self.holders[holder_id]
            logger.debug(
                f"Permit released on '{self.semaphore_id}' by {holder_id} (active={len(self.holders)}/{self.total_permits})"
            )

            # Wake up next pending waiter if permits available
            while self._wait_queue and len(self.holders) < self.total_permits:
                next_holder, next_ttl, next_fut = self._wait_queue.pop(0)
                if not next_fut.done():
                    self.holders[next_holder] = time.time() + next_ttl
                    next_fut.set_result(True)
                    break

            return True

    def to_spec(self) -> DistributedSemaphoreSpec:
        return DistributedSemaphoreSpec(
            semaphore_id=self.semaphore_id,
            total_permits=self.total_permits,
            available_permits=self.available_permits,
            holders=dict(self.holders),
            default_ttl_seconds=self.default_ttl_seconds,
        )


class SynchronizationManager:
    """
    Node-level coordinator for distributed barriers, latches, and counting semaphores.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._barriers: Dict[str, DistributedBarrier] = {}
        self._latches: Dict[str, DistributedCountdownLatch] = {}
        self._semaphores: Dict[str, DistributedSemaphore] = {}
        self._lock = asyncio.Lock()

    def get_or_create_barrier(
        self,
        barrier_id: str,
        threshold: int,
        timeout_seconds: float = 30.0,
    ) -> DistributedBarrier:
        if barrier_id not in self._barriers:
            self._barriers[barrier_id] = DistributedBarrier(
                barrier_id=barrier_id,
                threshold=threshold,
                timeout_seconds=timeout_seconds,
            )
        return self._barriers[barrier_id]

    def get_or_create_latch(self, latch_id: str, count: int) -> DistributedCountdownLatch:
        if latch_id not in self._latches:
            self._latches[latch_id] = DistributedCountdownLatch(latch_id=latch_id, count=count)
        return self._latches[latch_id]

    def get_or_create_semaphore(
        self,
        semaphore_id: str,
        total_permits: int,
        default_ttl_seconds: float = 30.0,
    ) -> DistributedSemaphore:
        if semaphore_id not in self._semaphores:
            self._semaphores[semaphore_id] = DistributedSemaphore(
                semaphore_id=semaphore_id,
                total_permits=total_permits,
                default_ttl_seconds=default_ttl_seconds,
            )
        return self._semaphores[semaphore_id]

    def get_barrier(self, barrier_id: str) -> Optional[DistributedBarrier]:
        return self._barriers.get(barrier_id)

    def get_semaphore(self, semaphore_id: str) -> Optional[DistributedSemaphore]:
        return self._semaphores.get(semaphore_id)

    def get_all_barrier_specs(self) -> List[DistributedBarrierSpec]:
        return [b.to_spec() for b in self._barriers.values()]

    def get_all_semaphore_specs(self) -> List[DistributedSemaphoreSpec]:
        return [s.to_spec() for s in self._semaphores.values()]
