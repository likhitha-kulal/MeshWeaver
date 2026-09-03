"""
Unit Tests for CircuitBreaker State Transitions and Tripping.
"""

import asyncio
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

    def test_async_guard_records_success_and_failure(self):
        async def run_test():
            cb = CircuitBreaker("node-1", CircuitBreakerConfig(failure_threshold=2))

            async with cb.async_guard():
                pass
            assert cb.failure_count == 0

            with pytest.raises(RuntimeError):
                async with cb.async_guard():
                    raise RuntimeError("Async boom")
            assert cb.failure_count == 1

        asyncio.run(run_test())


class TestCircuitBreakerHalfOpenAndRegistry:
    def test_transitions_to_half_open_after_recovery_timeout(self):
        cb = CircuitBreaker(
            "node-1",
            CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=0.05,
                half_open_success_threshold=2
            )
        )
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        time.sleep(0.06)
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.is_available() is True

        # Successful probe 1
        cb.record_success()
        assert cb.state == CircuitState.HALF_OPEN

        # Successful probe 2 closes the circuit
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    def test_half_open_probe_failure_trips_back_to_open(self):
        cb = CircuitBreaker(
            "node-1",
            CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=0.05
            )
        )
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        time.sleep(0.06)
        assert cb.state == CircuitState.HALF_OPEN

        # Failure during probe immediately trips back to OPEN
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_registry_management_and_query(self):
        registry = CircuitBreakerRegistry(CircuitBreakerConfig(failure_threshold=2))
        cb1 = registry.get_or_create("node-a")
        cb2 = registry.get_or_create("node-b")

        assert registry.is_node_available("node-a") is True
        assert registry.is_node_available("node-b") is True

        registry.record_node_failure("node-a")
        registry.record_node_failure("node-a")

        assert registry.is_node_available("node-a") is False
        assert registry.is_node_available("node-b") is True
        assert registry.get_tripped_nodes() == ["node-a"]

        registry.clear()
        assert len(registry.get_tripped_nodes()) == 0
