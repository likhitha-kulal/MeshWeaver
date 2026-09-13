"""
Unit tests for MeshWeaver Chaos Engine: PacketDropRule and LatencyJitterInjector (Week 4 Day 5).
"""

import asyncio
import unittest

from meshweaver.chaos import ChaosEngine, ChaosMetrics, LatencyJitterInjector, PacketDropRule
from meshweaver.models import ChaosConfig, Message, MessageType


class TestChaosPacketDropAndLatency(unittest.IsolatedAsyncioTestCase):
    def test_packet_drop_rule_all_matches(self):
        rule = PacketDropRule(rule_id="drop_all_pings", drop_rate=1.0, message_types={MessageType.PING})
        msg_ping = Message(msg_id="m1", type=MessageType.PING, sender_id="node_a", sender_udp_port=9000, payload={})
        msg_pong = Message(msg_id="m2", type=MessageType.PONG, sender_id="node_a", sender_udp_port=9000, payload={})

        self.assertTrue(rule.matches(msg_ping))
        self.assertFalse(rule.matches(msg_pong))

    def test_packet_drop_rule_source_and_target_filter(self):
        rule = PacketDropRule(
            rule_id="drop_a_to_b",
            drop_rate=1.0,
            source_nodes={"node_a"},
            target_nodes={"node_b"},
        )
        msg = Message(msg_id="m1", type=MessageType.GOSSIP, sender_id="node_a", sender_udp_port=9000, payload={})

        self.assertTrue(rule.matches(msg, sender_id="node_a", recipient_id="node_b"))
        self.assertFalse(rule.matches(msg, sender_id="node_a", recipient_id="node_c"))
        self.assertFalse(rule.matches(msg, sender_id="node_c", recipient_id="node_b"))

    def test_packet_drop_max_drops_limit(self):
        rule = PacketDropRule(rule_id="drop_2_pings", drop_rate=1.0, max_drops=2)
        msg = Message(msg_id="m1", type=MessageType.PING, sender_id="node_a", sender_udp_port=9000, payload={})

        self.assertTrue(rule.matches(msg))
        rule.record_drop()
        self.assertTrue(rule.matches(msg))
        rule.record_drop()

        # Should no longer match after 2 drops
        self.assertFalse(rule.matches(msg))
        self.assertFalse(rule.is_active)

    def test_latency_jitter_injector_bounds(self):
        injector = LatencyJitterInjector(min_latency_ms=10.0, max_latency_ms=20.0, jitter_ms=2.0)
        for _ in range(50):
            delay_sec = injector.calculate_delay_seconds()
            delay_ms = delay_sec * 1000.0
            self.assertTrue(delay_ms >= 8.0, f"Delay {delay_ms} < min 8.0ms")
            self.assertTrue(delay_ms <= 22.0, f"Delay {delay_ms} > max 22.0ms")

    async def test_async_latency_injection(self):
        injector = LatencyJitterInjector(min_latency_ms=15.0, max_latency_ms=25.0)
        start = asyncio.get_event_loop().time()
        injected_ms = await injector.inject_delay()
        elapsed = asyncio.get_event_loop().time() - start

        self.assertTrue(injected_ms >= 15.0)
        self.assertTrue(elapsed >= 0.010)

    def test_chaos_metrics_serialization(self):
        m = ChaosMetrics(
            total_packets_inspected=100,
            total_packets_dropped=15,
            total_packets_delayed=40,
            total_packets_corrupted=5,
            total_flaky_rpc_aborted=2,
            total_latency_injected_ms=250.0,
            active_partitions_count=1,
        )
        d = m.to_dict()
        self.assertEqual(d["total_packets_inspected"], 100)
        self.assertEqual(d["total_packets_dropped"], 15)
        self.assertEqual(d["active_partitions_count"], 1)


if __name__ == "__main__":
    unittest.main()
