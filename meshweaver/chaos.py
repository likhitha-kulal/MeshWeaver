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


class ChaosEngine:
    """
    Central Chaos & Reliability Engineering subsystem.
    Hooks directly into the Node networking layer to intercept, delay, drop, or corrupt
    datagrams and simulate complex multi-node failure scenarios.
    """

    def __init__(
        self,
        node_id: str,
        config: Optional[ChaosConfig] = None,
    ):
        self.node_id = node_id
        self.config = config if config is not None else ChaosConfig()
        self.metrics = ChaosMetrics()
        self.partitions: Dict[str, NetworkPartition] = {}
        self.drop_rules: Dict[str, PacketDropRule] = {}
        self.latency_injector = LatencyJitterInjector(
            min_latency_ms=self.config.min_latency_ms,
            max_latency_ms=self.config.max_latency_ms,
        )
        self._byzantine_callbacks: List[Callable[[Message], Message]] = []

    def enable(self) -> None:
        """Enable chaos fault injection."""
        self.config.enabled = True

    def disable(self) -> None:
        """Disable chaos fault injection."""
        self.config.enabled = False

    def is_enabled(self) -> bool:
        return self.config.enabled

    # --- Network Partitioning & Split-Brain Mechanics ---

    def create_partition(
        self,
        partition_id: str,
        group_a: Set[str],
        group_b: Set[str],
        bidirectional: bool = True,
    ) -> NetworkPartition:
        """
        Create a simulated network partition between two groups of nodes.
        Any message crossing group_a <-> group_b will be dropped.
        """
        partition = NetworkPartition(
            partition_id=partition_id,
            group_a=group_a,
            group_b=group_b,
            bidirectional=bidirectional,
        )
        self.partitions[partition_id] = partition
        self.metrics.active_partitions_count = len([p for p in self.partitions.values() if p.is_active])
        self.config.enabled = True
        logger.warning(
            f"[CHAOS PARTITION] Created partition '{partition_id}' between "
            f"Group A ({len(group_a)} nodes) and Group B ({len(group_b)} nodes)"
        )
        return partition

    def isolate_node(self, target_node_id: str) -> NetworkPartition:
        """Completely isolate a target node from all other cluster members (island mode)."""
        self.config.isolated_nodes.add(target_node_id)
        partition_id = f"isolate_{target_node_id[:8]}"
        partition = NetworkPartition(
            partition_id=partition_id,
            group_a={target_node_id},
            group_b=set(),  # Checked specially in is_partitioned
            bidirectional=True,
        )
        self.partitions[partition_id] = partition
        self.metrics.active_partitions_count = len([p for p in self.partitions.values() if p.is_active])
        self.config.enabled = True
        logger.warning(f"[CHAOS ISOLATION] Isolated node {target_node_id} from cluster")
        return partition

    def heal_partition(self, partition_id: str) -> bool:
        """Remove a specific partition rule and restore connectivity."""
        if partition_id in self.partitions:
            part = self.partitions.pop(partition_id)
            part.is_active = False
            self.metrics.active_partitions_count = len([p for p in self.partitions.values() if p.is_active])
            logger.info(f"[CHAOS HEAL] Healed network partition '{partition_id}'")
            return True
        return False

    def heal_all(self) -> None:
        """Heal all active network partitions, clear isolated nodes, and clear drop rules."""
        self.partitions.clear()
        self.config.isolated_nodes.clear()
        self.drop_rules.clear()
        self.metrics.active_partitions_count = 0
        logger.info("[CHAOS HEAL] All partitions and drop rules healed.")

    def is_partitioned(self, sender_id: str, recipient_id: str) -> bool:
        """Check if communication between sender and recipient is blocked by active partitions."""
        if sender_id in self.config.isolated_nodes or recipient_id in self.config.isolated_nodes:
            if sender_id != recipient_id:
                return True

        for partition in self.partitions.values():
            if partition.should_drop(sender_id, recipient_id):
                return True
        return False

    # --- Rule Management ---

    def add_drop_rule(self, rule: PacketDropRule) -> None:
        """Register a custom packet drop rule."""
        self.drop_rules[rule.rule_id] = rule
        self.config.enabled = True

    def remove_drop_rule(self, rule_id: str) -> bool:
        """Remove a custom packet drop rule."""
        return self.drop_rules.pop(rule_id, None) is not None

    def set_latency(self, min_ms: float, max_ms: float, jitter_ms: float = 0.0) -> None:
        """Configure artificial latency and jitter."""
        self.config.min_latency_ms = min_ms
        self.config.max_latency_ms = max_ms
        self.latency_injector = LatencyJitterInjector(min_ms, max_ms, jitter_ms)
        if max_ms > 0:
            self.config.enabled = True

    def set_packet_loss(self, loss_rate: float) -> None:
        """Set global random packet loss rate (0.0 to 1.0)."""
        self.config.packet_loss_rate = max(0.0, min(1.0, loss_rate))
        if self.config.packet_loss_rate > 0.0:
            self.config.enabled = True

    def set_byzantine_corruption(self, corruption_rate: float) -> None:
        """Configure probability of mutating message payloads (Byzantine fault simulation)."""
        self.config.byzantine_corruption_rate = max(0.0, min(1.0, corruption_rate))
        if self.config.byzantine_corruption_rate > 0.0:
            self.config.enabled = True

    def set_flaky_rpc_rate(self, flaky_rate: float) -> None:
        """Configure probability of RPC network timeout / connection abort."""
        self.config.flaky_rpc_rate = max(0.0, min(1.0, flaky_rate))
        if self.config.flaky_rpc_rate > 0.0:
            self.config.enabled = True

    def register_byzantine_mutator(self, callback: Callable[[Message], Message]) -> None:
        """Register custom function to mutate / tamper with outgoing payloads."""
        self._byzantine_callbacks.append(callback)
        self.config.enabled = True

    # --- Packet Processing & Interception Pipeline ---

    def evaluate_packet(
        self,
        msg: Message,
        sender_id: str,
        recipient_id: Optional[str] = None,
    ) -> Tuple[Optional[Message], float, bool]:
        """
        Evaluate an inbound or outbound message against all active chaos rules.
        Returns (processed_message, delay_seconds, should_drop).
        """
        if not self.config.enabled:
            return msg, 0.0, False

        self.metrics.total_packets_inspected += 1

        # 1. Network Partition Check
        if recipient_id and self.is_partitioned(sender_id, recipient_id):
            self.metrics.total_packets_dropped += 1
            logger.debug(f"[CHAOS DROP] Dropped packet {msg.type.value} due to partition ({sender_id} -> {recipient_id})")
            return None, 0.0, True

        # 2. Rule-Based Drop Check
        for rule in list(self.drop_rules.values()):
            if rule.matches(msg, sender_id, recipient_id):
                rule.record_drop()
                self.metrics.total_packets_dropped += 1
                logger.debug(f"[CHAOS DROP] Dropped packet {msg.type.value} matching rule '{rule.rule_id}'")
                return None, 0.0, True

        # 3. Probabilistic Random Packet Loss
        if self.config.packet_loss_rate > 0.0 and random.random() < self.config.packet_loss_rate:
            self.metrics.total_packets_dropped += 1
            logger.debug(f"[CHAOS DROP] Dropped packet {msg.type.value} due to random loss ({self.config.packet_loss_rate:.1%})")
            return None, 0.0, True

        # 4. Latency Jitter Calculation
        delay_sec = self.latency_injector.calculate_delay_seconds()
        if delay_sec > 0.0:
            self.metrics.total_packets_delayed += 1
            self.metrics.total_latency_injected_ms += delay_sec * 1000.0

        # 5. Byzantine Payload Corruption
        processed_msg = msg
        if self.config.byzantine_corruption_rate > 0.0 and random.random() < self.config.byzantine_corruption_rate:
            self.metrics.total_packets_corrupted += 1
            processed_msg = self._corrupt_message(msg)
            logger.warning(f"[CHAOS BYZANTINE] Injected Byzantine corruption into packet {msg.type.value}")

        for cb in self._byzantine_callbacks:
            processed_msg = cb(processed_msg)

        return processed_msg, delay_sec, False

    def _corrupt_message(self, msg: Message) -> Message:
        """Mutate message payload or term to simulate malicious/corrupted transmission."""
        corrupted_payload = dict(msg.payload)
        corrupted_payload["__byzantine_corrupted__"] = True
        if "term" in corrupted_payload and isinstance(corrupted_payload["term"], int):
            corrupted_payload["term"] = corrupted_payload["term"] + 999
        if "entries" in corrupted_payload and isinstance(corrupted_payload["entries"], list):
            corrupted_payload["entries"] = []  # Truncate entries
        return Message(
            msg_id=msg.msg_id,
            type=msg.type,
            sender_id=msg.sender_id,
            sender_udp_port=msg.sender_udp_port,
            payload=corrupted_payload,
            timestamp=msg.timestamp,
        )

    def should_abort_flaky_rpc(self) -> bool:
        """Check if an RPC call should fail with a simulated flaky network exception."""
        if not self.config.enabled or self.config.flaky_rpc_rate <= 0.0:
            return False
        if random.random() < self.config.flaky_rpc_rate:
            self.metrics.total_flaky_rpc_aborted += 1
            return True
        return False

    def get_metrics(self) -> ChaosMetrics:
        """Retrieve live operational telemetry of the Chaos Engine."""
        self.metrics.active_partitions_count = len([p for p in self.partitions.values() if p.is_active])
        return self.metrics


