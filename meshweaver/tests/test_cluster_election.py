"""
Integration tests for multi-node MeshWeaver cluster leader election and failover.
"""

import asyncio
import unittest

from meshweaver.models import NodeID
from meshweaver.node import MeshNode


class TestClusterElectionIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_three_node_cluster_leader_election(self):
        node1 = MeshNode(host="127.0.0.1", udp_port=19200, tcp_port=19201)
        node2 = MeshNode(host="127.0.0.1", udp_port=19210, tcp_port=19211)
        node3 = MeshNode(host="127.0.0.1", udp_port=19220, tcp_port=19221)
        
        nodes = [node1, node2, node3]
        for n in nodes:
            await n.start()
            n.leader_election.config.min_election_timeout = 0.150
            n.leader_election.config.max_election_timeout = 0.300
            n.leader_election.config.heartbeat_interval = 0.040

        try:
            # Connect all nodes to each other
            await node2.bootstrap([("127.0.0.1", 19200)])
            await node3.bootstrap([("127.0.0.1", 19200)])
            await node1.bootstrap([("127.0.0.1", 19210)])
            await asyncio.sleep(0.15)

            await node1.trigger_election()
            await asyncio.sleep(0.5)

            leaders = [n for n in nodes if n.is_leader]
            self.assertTrue(len(leaders) >= 1)
            leader_id = leaders[0].node_id.hex()

            recognized = sum(1 for n in nodes if n.leader_id == leader_id)
            self.assertTrue(recognized >= 2)

        finally:
            for n in nodes:
                await n.stop()

    async def test_cluster_leader_failover(self):
        node1 = MeshNode(host="127.0.0.1", udp_port=19300, tcp_port=19301)
        node2 = MeshNode(host="127.0.0.1", udp_port=19310, tcp_port=19311)
        
        nodes = [node1, node2]
        for n in nodes:
            await n.start()
            n.leader_election.config.min_election_timeout = 0.150
            n.leader_election.config.max_election_timeout = 0.250
            n.leader_election.config.heartbeat_interval = 0.040

        try:
            await node2.bootstrap([("127.0.0.1", 19300)])
            await node1.bootstrap([("127.0.0.1", 19310)])
            await asyncio.sleep(0.1)

            await node1.trigger_election()
            await asyncio.sleep(0.3)
            self.assertTrue(node1.is_leader or node2.is_leader)

            if node1.is_leader:
                await node1.stop()
                node2.routing_table.remove_contact(node1.node_id)
                node2.gossip_manager.neighbors.pop(node1.node_id.hex(), None)
                node2.gossip_manager.peer_loads.pop(node1.node_id.hex(), None)
                await node2.trigger_election()
                await asyncio.sleep(0.35)
                self.assertTrue(node2.is_leader)
            else:
                await node2.stop()
                node1.routing_table.remove_contact(node2.node_id)
                node1.gossip_manager.neighbors.pop(node2.node_id.hex(), None)
                node1.gossip_manager.peer_loads.pop(node2.node_id.hex(), None)
                await node1.trigger_election()
                await asyncio.sleep(0.35)
                self.assertTrue(node1.is_leader)

        finally:
            for n in [node1, node2]:
                try:
                    await n.stop()
                except Exception:
                    pass


if __name__ == "__main__":
    unittest.main()
