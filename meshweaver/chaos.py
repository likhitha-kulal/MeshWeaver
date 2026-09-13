"""
MeshWeaver Chaos Resilience & Network Fault Injection Engine (Week 4 Day 5).
Provides synthetic packet dropping, latency jitter injection, Byzantine payload corruption,
and dynamic cluster network partitioning (split-brain) simulation for reliability verification.
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from meshweaver.models import ChaosConfig, FaultType, Message, MessageType, NetworkPartition

logger = logging.getLogger("meshweaver.chaos")


@dataclass
class ChaosMetrics:
    """Telemetry counters for synthetic faults injected during testing."""
    total_packets_inspected: int = 0
    total_packets_dropped: int = 0
    total_packets_delayed: int = 0
    total_packets_corrupted: int = 0
    total_flaky_rpc_aborted: int = 0
    total_latency_injected_ms: float = 0.0
    active_partitions_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_packets_inspected": self.total_packets_inspected,
            "total_packets_dropped": self.total_packets_dropped,
            "total_packets_delayed": self.total_packets_delayed,
            "total_packets_corrupted": self.total_packets_corrupted,
            "total_flaky_rpc_aborted": self.total_flaky_rpc_aborted,
            "total_latency_injected_ms": self.total_latency_injected_ms,
            "active_partitions_count": self.active_partitions_count,
        }


class PacketDropRule:
    """
    Fine-grained packet filter rule deciding whether an incoming or outgoing datagram is dropped.
    Supports filtering by message type, target sender/recipient, and drop probability.
    """

    def __init__(
        self,
        rule_id: str,
        drop_rate: float = 1.0,
        message_types: Optional[Set[MessageType]] = None,
        source_nodes: Optional[Set[str]] = None,
        target_nodes: Optional[Set[str]] = None,
        max_drops: Optional[int] = None,
    ):
        self.rule_id = rule_id
        self.drop_rate = max(0.0, min(1.0, drop_rate))
        self.message_types = message_types
        self.source_nodes = source_nodes
        self.target_nodes = target_nodes
        self.max_drops = max_drops
        self.drop_count = 0
        self.is_active = True

    def matches(self, msg: Message, sender_id: Optional[str] = None, recipient_id: Optional[str] = None) -> bool:
        """Evaluate if the message matches the criteria of this drop rule."""
        if not self.is_active:
            return False

        if self.max_drops is not None and self.drop_count >= self.max_drops:
            self.is_active = False
            return False

        if self.message_types is not None and msg.type not in self.message_types:
            return False

        sender = sender_id or getattr(msg, "sender_id", None)
        if self.source_nodes is not None and sender not in self.source_nodes:
            return False

        recipient = recipient_id or getattr(msg, "recipient_id", None)
        if self.target_nodes is not None and recipient not in self.target_nodes:
            return False

        return random.random() < self.drop_rate

    def record_drop(self) -> None:
        """Increment drop counter."""
        self.drop_count += 1
        if self.max_drops is not None and self.drop_count >= self.max_drops:
            self.is_active = False


class LatencyJitterInjector:
    """
    Artificial network latency and jitter injector.
    Simulates high-latency WAN links, congestion jitter, and slow transport hops.
    """

    def __init__(
        self,
        min_latency_ms: float = 0.0,
        max_latency_ms: float = 0.0,
        jitter_ms: float = 0.0,
    ):
        self.min_latency_ms = max(0.0, min_latency_ms)
        self.max_latency_ms = max(self.min_latency_ms, max_latency_ms)
        self.jitter_ms = max(0.0, jitter_ms)

    def calculate_delay_seconds(self) -> float:
        """Compute randomized artificial delay in seconds based on min, max, and jitter parameters."""
        if self.max_latency_ms <= 0.0:
            return 0.0

        base_delay_ms = random.uniform(self.min_latency_ms, self.max_latency_ms)
        if self.jitter_ms > 0:
            jitter = random.uniform(-self.jitter_ms, self.jitter_ms)
            base_delay_ms = max(0.0, base_delay_ms + jitter)

        return base_delay_ms / 1000.0

    async def inject_delay(self) -> float:
        """Asynchronously sleep for the computed artificial delay duration."""
        delay = self.calculate_delay_seconds()
        if delay > 0.0:
            await asyncio.sleep(delay)
        return delay * 1000.0
