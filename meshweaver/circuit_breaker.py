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
