"""
Circuit Breaker Pattern for MeshWeaver Distributed Nodes.

Prevents cascading cluster failures by isolating failing, overloaded, or
unresponsive worker nodes.
"""

import contextlib
from dataclasses import dataclass
from enum import Enum
import time
from typing import AsyncIterator, Dict, Iterator, List, Optional


class CircuitState(Enum):
    """Possible states for a node's circuit breaker."""
    CLOSED = "CLOSED"        # Normal operation: requests pass through
    OPEN = "OPEN"            # Faulty node: requests fast-fail immediately
    HALF_OPEN = "HALF_OPEN"  # Probe state: allows trial requests to test recovery


@dataclass
class CircuitBreakerConfig:
    """Configuration parameters for CircuitBreaker behavior."""
    failure_threshold: int = 3          # Consecutive failures before tripping OPEN
    recovery_timeout: float = 5.0       # Seconds to wait in OPEN state before testing HALF_OPEN
    half_open_success_threshold: int = 2  # Successful probes required in HALF_OPEN to CLOSE
    probe_concurrency: int = 1          # Max concurrent test requests in HALF_OPEN state


class CircuitBreakerOpenError(Exception):
    """Raised when an operation is attempted on a target whose circuit breaker is OPEN."""
    pass


class CircuitBreaker:
    """
    Per-node Circuit Breaker tracking operational health and tripping state.
    """

    def __init__(self, node_id_hex: str, config: Optional[CircuitBreakerConfig] = None):
        self.node_id_hex = node_id_hex
        self.config = config or CircuitBreakerConfig()
        self._state: CircuitState = CircuitState.CLOSED
        self._failure_count: int = 0
        self._success_count: int = 0
        self._last_state_change: float = time.time()
        self._last_failure_time: Optional[float] = None
        self._active_probes: int = 0

    @property
    def state(self) -> CircuitState:
        """Evaluates current state with automatic transition from OPEN to HALF_OPEN on timeout."""
        if self._state == CircuitState.OPEN:
            elapsed = time.time() - self._last_state_change
            if elapsed >= self.config.recovery_timeout:
                self._transition_to(CircuitState.HALF_OPEN)
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    @property
    def success_count(self) -> int:
        return self._success_count

    def _transition_to(self, new_state: CircuitState) -> None:
        """Performs internal state transition and resets relevant transient counters."""
        self._state = new_state
        self._last_state_change = time.time()
        if new_state == CircuitState.HALF_OPEN:
            self._success_count = 0
            self._active_probes = 0
        elif new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
            self._active_probes = 0
        elif new_state == CircuitState.OPEN:
            self._active_probes = 0

    def is_available(self) -> bool:
        """Returns True if the node can accept new task requests."""
        current_state = self.state
        if current_state == CircuitState.CLOSED:
            return True
        if current_state == CircuitState.HALF_OPEN:
            return self._active_probes < self.config.probe_concurrency
        return False

    def record_failure(self, error: Optional[Exception] = None) -> None:
        """Records an operational failure. Trips circuit to OPEN if threshold exceeded."""
        self._last_failure_time = time.time()
        current_state = self.state

        if current_state == CircuitState.HALF_OPEN:
            # Any failure during half-open probe immediately trips back to OPEN
            self._transition_to(CircuitState.OPEN)
        elif current_state == CircuitState.CLOSED:
            self._failure_count += 1
            if self._failure_count >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)

    def record_success(self) -> None:
        """Records a successful operation."""
        current_state = self.state
        if current_state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.half_open_success_threshold:
                self._transition_to(CircuitState.CLOSED)
        elif current_state == CircuitState.CLOSED:
            self._failure_count = max(0, self._failure_count - 1)

    @contextlib.contextmanager
    def guard(self) -> Iterator[None]:
        """
        Synchronous context manager that gates execution through the circuit breaker.
        """
        if not self.is_available():
            raise CircuitBreakerOpenError(
                f"Circuit breaker for node {self.node_id_hex[:8]} is {self.state.value}"
            )

        if self.state == CircuitState.HALF_OPEN:
            self._active_probes += 1

        try:
            yield
            self.record_success()
        except Exception as e:
            self.record_failure(e)
            raise
        finally:
            if self.state == CircuitState.HALF_OPEN and self._active_probes > 0:
                self._active_probes -= 1

    @contextlib.asynccontextmanager
    async def async_guard(self) -> AsyncIterator[None]:
        """
        Asynchronous context manager that gates execution through the circuit breaker.
        """
        if not self.is_available():
            raise CircuitBreakerOpenError(
                f"Circuit breaker for node {self.node_id_hex[:8]} is {self.state.value}"
            )

        if self.state == CircuitState.HALF_OPEN:
            self._active_probes += 1

        try:
            yield
            self.record_success()
        except Exception as e:
            self.record_failure(e)
            raise
        finally:
            if self.state == CircuitState.HALF_OPEN and self._active_probes > 0:
                self._active_probes -= 1


class CircuitBreakerRegistry:
    """
    Central registry of CircuitBreakers keyed by node_id_hex.
    """

    def __init__(self, default_config: Optional[CircuitBreakerConfig] = None):
        self.default_config = default_config or CircuitBreakerConfig()
        self._breakers: Dict[str, CircuitBreaker] = {}

    def get_or_create(self, node_id_hex: str) -> CircuitBreaker:
        """Retrieves or creates a CircuitBreaker instance for the specified node."""
        if node_id_hex not in self._breakers:
            self._breakers[node_id_hex] = CircuitBreaker(node_id_hex, self.default_config)
        return self._breakers[node_id_hex]

    def is_node_available(self, node_id_hex: str) -> bool:
        """Convenience method checking if a node is currently accepting requests."""
        if node_id_hex not in self._breakers:
            return True
        return self._breakers[node_id_hex].is_available()

    def record_node_failure(self, node_id_hex: str, error: Optional[Exception] = None) -> None:
        """Records a failure for the specified node."""
        self.get_or_create(node_id_hex).record_failure(error)

    def record_node_success(self, node_id_hex: str) -> None:
        """Records a success for the specified node."""
        self.get_or_create(node_id_hex).record_success()

    def get_tripped_nodes(self) -> List[str]:
        """Returns a list of node IDs whose circuits are currently OPEN."""
        return [nid for nid, cb in self._breakers.items() if cb.state == CircuitState.OPEN]

    def clear(self) -> None:
        """Clears all registered circuit breakers."""
        self._breakers.clear()
