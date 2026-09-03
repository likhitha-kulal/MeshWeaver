"""
Circuit Breaker Pattern for MeshWeaver Distributed Nodes.

Prevents cascading cluster failures by isolating failing, overloaded, or
unresponsive worker nodes.
"""

from dataclasses import dataclass
from enum import Enum
import time
from typing import Dict, Optional


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
