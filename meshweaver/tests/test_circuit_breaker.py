"""
Unit Tests for CircuitBreaker State Transitions and Tripping.
"""

import time
import pytest
from meshweaver.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitBreakerRegistry,
    CircuitState,
)


class TestCircuitBreakerStateTransitions:
    def test_initial_state_is_closed(self):
        cb = CircuitBreaker("node-1", CircuitBreakerConfig(failure_threshold=3))
        assert cb.state == CircuitState.CLOSED
        assert cb.is_available() is True
        assert cb.failure_count == 0

    def test_trips_to_open_after_threshold_failures(self):
        cb = CircuitBreaker("node-1", CircuitBreakerConfig(failure_threshold=3, recovery_timeout=1.0))
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 1

        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 2

        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.is_available() is False

    def test_guard_raises_circuit_breaker_open_error(self):
        cb = CircuitBreaker("node-1", CircuitBreakerConfig(failure_threshold=2))
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        with pytest.raises(CircuitBreakerOpenError):
            with cb.guard():
                pass

    def test_guard_records_success_and_failure(self):
        cb = CircuitBreaker("node-1", CircuitBreakerConfig(failure_threshold=2))

        # Successful execution
        with cb.guard():
            pass
        assert cb.failure_count == 0

        # Exception execution
        with pytest.raises(ValueError):
            with cb.guard():
                raise ValueError("Boom")
        assert cb.failure_count == 1
