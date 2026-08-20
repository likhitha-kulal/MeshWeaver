"""
Tests for Gossip protocol, heartbeat health tracking, and load balancer peer selection.
"""

import asyncio
import unittest

from meshweaver.gossip import GossipManager, PeerLoadSnapshot
from meshweaver.node import MeshNode


class TestGossip(unittest.IsolatedAsyncioTestCase):

    def test_least_loaded_peer_selection(self):
        manager = GossipManager(node_id="node_001", host="127.0.0.1", udp_port=9300)

        manager.peer_loads["peer_high"] = PeerLoadSnapshot(
            node_id="peer_high", ip="127.0.0.1", udp_port=9301,
            cpu_percent=80.0, ram_percent=85.0,
        )
        manager.peer_loads["peer_low"] = PeerLoadSnapshot(
            node_id="peer_low", ip="127.0.0.1", udp_port=9302,
            cpu_percent=15.0, ram_percent=20.0,
        )
        manager.peer_loads["peer_mid"] = PeerLoadSnapshot(
            node_id="peer_mid", ip="127.0.0.1", udp_port=9303,
            cpu_percent=50.0, ram_percent=50.0,
        )

        selected = manager.get_least_loaded_peer()
        self.assertIsNotNone(selected)
        self.assertEqual(selected.node_id, "peer_low")

    def test_dead_node_expiration(self):
        manager = GossipManager(node_id="node_002", heartbeat_interval=0.01, dead_node_timeout=0.2)
        manager.register_neighbor("peer_dead", "127.0.0.1", 9310)

        manager.receive_heartbeat({
            "sender_id": "peer_dead",
            "ip": "127.0.0.1",
            "udp_port": 9310,
            "cpu_percent": 30.0,
            "ram_percent": 40.0,
            "timestamp": 1000.0,
        })
        self.assertIn("peer_dead", manager.peer_loads)

        manager.peer_loads["peer_dead"].timestamp = 0.0
        evicted = manager.expire_dead_nodes(now=1000.5)
        self.assertIn("peer_dead", evicted)
        self.assertNotIn("peer_dead", manager.peer_loads)

    async def test_multi_node_cluster_bootstrap(self):
        node_a = MeshNode(host="127.0.0.1", udp_port=18100, tcp_port=18101)
        node_b = MeshNode(host="127.0.0.1", udp_port=18102, tcp_port=18103)
        node_c = MeshNode(host="127.0.0.1", udp_port=18104, tcp_port=18105)

        await node_a.start()
        await node_b.start()
        await node_c.start()

        try:
            # Node B registers with bootstrap node A
            await node_b.ping("127.0.0.1", node_a.bound_udp_port)

            # Node C joins mesh via bootstrap node A
            await node_c.bootstrap([("127.0.0.1", node_a.bound_udp_port)], timeout=3.0)

            known_ids = {c.node_id.hex() for c in node_c.routing_table.get_all_contacts()}
            self.assertIn(node_a.node_id.hex(), known_ids)
            self.assertIn(node_b.node_id.hex(), known_ids)
        finally:
            await node_a.stop()
            await node_b.stop()
            await node_c.stop()


if __name__ == "__main__":
    unittest.main()

# Verified peer registration and telemetry snapshot
