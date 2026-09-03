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

    def test_exponential_recovery_backoff_on_repeated_trips(self):
        cb = CircuitBreaker(
            "node-1",
            CircuitBreakerConfig(
                failure_threshold=1,
                recovery_timeout=1.0,
                backoff_multiplier=2.0,
                max_recovery_timeout=10.0,
            )
        )
        # Trip 1: recovery timeout = 1.0 * (2^0) = 1.0
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.consecutive_trips == 1
        assert cb.current_recovery_timeout == 1.0

        # Trip 2: recovery timeout = 1.0 * (2^1) = 2.0
        cb._transition_to(CircuitState.HALF_OPEN)
        cb.record_failure()
        assert cb.consecutive_trips == 2
        assert cb.current_recovery_timeout == 2.0

        # Trip 3: recovery timeout = 1.0 * (2^2) = 4.0
        cb._transition_to(CircuitState.HALF_OPEN)
        cb.record_failure()
        assert cb.consecutive_trips == 3
        assert cb.current_recovery_timeout == 4.0

        # Reset on close
        cb._transition_to(CircuitState.CLOSED)
        assert cb.consecutive_trips == 0
        assert cb.current_recovery_timeout == 1.0

    def test_breaker_metrics_generation(self):
        cb = CircuitBreaker("node-test", CircuitBreakerConfig(failure_threshold=2))
        cb.record_failure()
        metrics = cb.get_metrics()

        assert metrics.node_id == "node-test"
        assert metrics.state == "CLOSED"
        assert metrics.failure_count == 1
        assert metrics.success_count == 0
        assert metrics.is_available is True
        assert metrics.failure_rate == 100.0

    def test_registry_remove_and_reset_all(self):
        registry = CircuitBreakerRegistry()
        registry.get_or_create("node-1")
        registry.get_or_create("node-2")
        assert registry.registered_count == 2

        registry.record_node_failure("node-1")
        registry.record_node_failure("node-1")
        registry.record_node_failure("node-1")

        all_m = registry.get_all_metrics()
        assert len(all_m) == 2
        assert all_m["node-1"].state == "OPEN"

        # Test reset_all
        registry.reset_all()
        assert registry.get_or_create("node-1").state == CircuitState.CLOSED

        # Test remove_node
        assert registry.remove_node("node-1") is True
        assert registry.registered_count == 1
        assert registry.remove_node("non-existent") is False
