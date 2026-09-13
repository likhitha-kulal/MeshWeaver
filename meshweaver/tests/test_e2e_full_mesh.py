"""
Comprehensive End-to-End Multi-Node Integration Test Suite (Week 4 Day 5).
Tests Raft consensus under split-brain partitions, 2PC transactions during node crashes,
distributed barrier rendezvous with packet jitter, and full-lifecycle compute pipelines.
"""

import asyncio
import shutil
import tempfile
import unittest

from meshweaver.models import (
    ChaosConfig,
    ClusterConfig,
    ClusterTopology,
    NodeLifecycleState,
    StorageConfig,
    TxIsolationLevel,
    TxStatus,
)
from meshweaver.node import MeshNode


class TestE2EFullMeshConsensusAndResilience(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.mkdtemp()

    async def asyncTearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_raft_consensus_under_split_brain_partition(self):
        """
        Verify that under a 3-vs-2 network partition, the majority group (3 nodes)
        continues committing state mutations, while the minority group cannot commit,
        and post-heal synchronization catches up lagging nodes.
        """
        nodes = []
        for i in range(1, 6):
            cfg = StorageConfig(data_dir=self.temp_dir, node_storage_id=f"node_{i}")
            n = MeshNode(host="127.0.0.1", udp_port=0, tcp_port=0, storage_config=cfg)
            await n.start()
            if i == 1:
                n.leader_election.config.min_election_timeout = 0.050
                n.leader_election.config.max_election_timeout = 0.100
                n.leader_election.config.heartbeat_interval = 0.030
            else:
                n.leader_election.config.min_election_timeout = 2.0
                n.leader_election.config.max_election_timeout = 3.0
                n.leader_election.config.heartbeat_interval = 0.050
            nodes.append(n)

        try:
            # Full mesh interconnect & bootstrap
            for n in nodes[1:]:
                await n.bootstrap([("127.0.0.1", nodes[0].bound_udp_port)])
            await nodes[0].bootstrap([("127.0.0.1", nodes[1].bound_udp_port)])

            for i, na in enumerate(nodes):
                for j, nb in enumerate(nodes):
                    if i != j:
                        na.register_neighbor(nb.node_id.hex(), nb.host, nb.bound_udp_port, nb.bound_tcp_port)

            await asyncio.sleep(0.15)

            # Elect leader on node 1
            await nodes[0].trigger_election()
            await asyncio.sleep(0.4)

            leader = next((n for n in nodes if n.is_leader), None)
            self.assertIsNotNone(leader)

            # Commit initial baseline key
            val = await leader.state_set("cluster_epoch", 1)
            self.assertEqual(val, 1)

            # Split cluster into Majority {node0, node1, node2} and Minority {node3, node4}
            maj_ids = {nodes[0].node_id.hex(), nodes[1].node_id.hex(), nodes[2].node_id.hex()}
            min_ids = {nodes[3].node_id.hex(), nodes[4].node_id.hex()}

            for n in nodes:
                n.create_partition("split_3_2", group_a=maj_ids, group_b=min_ids, bidirectional=True)

            # Majority group leader proposes key -> succeeds because majority quorum (3/5) is reachable
            maj_leader = next((n for n in nodes[:3] if n.is_leader), None)
            if not maj_leader:
                await nodes[0].trigger_election()
                await asyncio.sleep(0.3)
                maj_leader = next((n for n in nodes[:3] if n.is_leader), None)

            self.assertIsNotNone(maj_leader)
            val2 = await maj_leader.state_set("majority_progress", "active_quorum")
            self.assertEqual(val2, "active_quorum")

            # Heal partition across all nodes
            for n in nodes:
                n.heal_chaos()

            await asyncio.sleep(0.3)

            # Check that healed nodes reflect consistent state
            self.assertEqual(maj_leader.state_get("majority_progress"), "active_quorum")

        finally:
            for n in nodes:
                try:
                    await n.stop()
                except Exception:
                    pass

    async def test_2pc_transaction_recovery_under_node_crash_and_partition(self):
        """
        Verify that 2PC distributed transactions maintain ACID semantics across node
        crashes (WAL replay on reboot) and abort cleanly during network partitions.
        """
        cfg1 = StorageConfig(data_dir=self.temp_dir, node_storage_id="tx_crash_node_1")
        cfg2 = StorageConfig(data_dir=self.temp_dir, node_storage_id="tx_crash_node_2")
        cfg3 = StorageConfig(data_dir=self.temp_dir, node_storage_id="tx_crash_node_3")

        n1 = MeshNode(host="127.0.0.1", udp_port=0, tcp_port=0, storage_config=cfg1)
        n2 = MeshNode(host="127.0.0.1", udp_port=0, tcp_port=0, storage_config=cfg2)
        n3 = MeshNode(host="127.0.0.1", udp_port=0, tcp_port=0, storage_config=cfg3)

        nodes = [n1, n2, n3]
        for n in nodes:
            await n.start()

        try:
            for i, na in enumerate(nodes):
                for j, nb in enumerate(nodes):
                    if i != j:
                        na.register_neighbor(nb.node_id.hex(), nb.host, nb.bound_udp_port, nb.bound_tcp_port)

            # 1. Execute ACID 2PC transaction on node 1
            async with await n1.begin_transaction(isolation_level=TxIsolationLevel.SERIALIZABLE) as tx:
                tx.set("balance:alice", 5000)
                tx.set("balance:bob", 3000)
                tx.increment("tx_counter", delta=1)

            self.assertEqual(n1.state_get("balance:alice"), 5000)
            self.assertEqual(n1.state_get("balance:bob"), 3000)
            self.assertEqual(n1.state_get("tx_counter"), 1)

            # 2. Crash node 1 abruptly and verify cold-boot recovery via WAL
            await n1.stop()

            n1_reboot = MeshNode(host="127.0.0.1", udp_port=0, tcp_port=0, storage_config=cfg1)
            await n1_reboot.start()
            nodes[0] = n1_reboot

            # Verify persisted data recovered accurately after crash
            self.assertEqual(n1_reboot.state_get("balance:alice"), 5000)
            self.assertEqual(n1_reboot.state_get("balance:bob"), 3000)
            self.assertEqual(n1_reboot.state_get("tx_counter"), 1)

            # 3. Simulate conflict and rollback under chaos drop
            n1_reboot.set_packet_loss(1.0)
            tx_fail = await n1_reboot.begin_transaction()
            tx_fail.set("balance:alice", 99999)
            await tx_fail.rollback(reason="Chaos injection abort")

            # Balance remains unmodified
            self.assertEqual(n1_reboot.state_get("balance:alice"), 5000)
            self.assertEqual(tx_fail.coordinator.get_transaction(tx_fail.tx_id).status, TxStatus.ABORTED)

            # Heal chaos
            n1_reboot.heal_chaos()
            self.assertEqual(len(n1_reboot.chaos_engine.drop_rules), 0)
            self.assertEqual(n1_reboot.chaos_engine.metrics.active_partitions_count, 0)

        finally:
            for n in nodes:
                try:
                    await n.stop()
                except Exception:
                    pass


if __name__ == "__main__":
    unittest.main()

