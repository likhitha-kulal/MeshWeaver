"""
MeshWeaver Task Scheduler Module.
Provides intelligent load-balanced task dispatching, retry policies,
and failover mechanisms across active mesh peer nodes.
"""

from dataclasses import dataclass, field
from enum import Enum
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("meshweaver.scheduler")


class SchedulingPolicy(str, Enum):
    """Supported task routing and worker selection policies."""
    LEAST_LOADED = "least_loaded"
    ROUND_ROBIN = "round_robin"
    POWER_OF_TWO_RANDOM = "power_of_two_random"
    LOCAL_FIRST = "local_first"


@dataclass
class RetryPolicy:
    """Configuration for handling remote dispatch failures and retries."""
    max_retries: int = 3
    backoff_factor: float = 0.5
    timeout_per_attempt: float = 5.0
    exclude_failed_nodes: bool = True
