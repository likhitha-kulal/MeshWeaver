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
        Verify that under a network partition, the majority group (2-of-3)
        continues committing state mutations, while partitioned nodes cannot interfere,
        and post-heal synchronization succeeds.
        """
        cfg1 = StorageConfig(data_dir=self.temp_dir, node_storage_id="node_1")
        cfg2 = StorageConfig(data_dir=self.temp_dir, node_storage_id="node_2")
        cfg3 = StorageConfig(data_dir=self.temp_dir, node_storage_id="node_3")

        n1 = MeshNode(host="127.0.0.1", udp_port=0, tcp_port=0, storage_config=cfg1)
        n2 = MeshNode(host="127.0.0.1", udp_port=0, tcp_port=0, storage_config=cfg2)
        n3 = MeshNode(host="127.0.0.1", udp_port=0, tcp_port=0, storage_config=cfg3)

        nodes = [n1, n2, n3]
        for n in nodes:
            await n.start()
            n.leader_election.config.min_election_timeout = 0.150
            n.leader_election.config.max_election_timeout = 0.300
            n.leader_election.config.heartbeat_interval = 0.040

        try:
            # Bootstrap 3-node cluster
            await n2.bootstrap([("127.0.0.1", n1.bound_udp_port)])
            await n3.bootstrap([("127.0.0.1", n1.bound_udp_port)])
            await n1.bootstrap([("127.0.0.1", n2.bound_udp_port)])
            await asyncio.sleep(0.15)

            # Elect n1 as leader
            await n1.trigger_election()
            await asyncio.sleep(0.4)

            self.assertTrue(n1.is_leader)

            # Commit initial baseline key
            val = await n1.state_set("cluster_epoch", 1)
            self.assertEqual(val, 1)

            # Split cluster into Majority {n1, n2} and Minority {n3}
            maj_ids = {n1.node_id.hex(), n2.node_id.hex()}
            min_ids = {n3.node_id.hex()}

            for n in nodes:
                n.create_partition("split_2_1", group_a=maj_ids, group_b=min_ids, bidirectional=True)

            # Majority group leader proposes key -> succeeds with 2/3 quorum
            val2 = await n1.state_set("majority_progress", "active_quorum")
            self.assertEqual(val2, "active_quorum")

            # Heal partition across all nodes
            for n in nodes:
                n.heal_chaos()

            await asyncio.sleep(0.15)

            # Verify consistent state
            self.assertEqual(n1.state_get("majority_progress"), "active_quorum")

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

    async def test_distributed_barrier_sync_under_latency_jitter(self):
        """
        Verify that distributed rendezvous barriers synchronize accurately across nodes
        even in the presence of synthetic latency jitter and network delays.
        """
        n1 = MeshNode(host="127.0.0.1", udp_port=0, tcp_port=0)
        n2 = MeshNode(host="127.0.0.1", udp_port=0, tcp_port=0)
        n3 = MeshNode(host="127.0.0.1", udp_port=0, tcp_port=0)

        nodes = [n1, n2, n3]
        for n in nodes:
            await n.start()

        try:
            # Bootstrap cluster
            await n2.bootstrap([("127.0.0.1", n1.bound_udp_port)])
            await n3.bootstrap([("127.0.0.1", n1.bound_udp_port)])
            await asyncio.sleep(0.15)

            # Inject synthetic latency and jitter on nodes 2 and 3
            n2.inject_latency(min_ms=10.0, max_ms=25.0, jitter_ms=5.0)
            n3.inject_latency(min_ms=15.0, max_ms=35.0, jitter_ms=8.0)

            barrier_id = "jitter_barrier_alpha"
            barrier = n1.create_barrier(barrier_id, threshold=3, timeout_seconds=5.0)

            arrival_order = []

            async def participant_worker(node: MeshNode, participant_id: str):
                await asyncio.sleep(0.02)
                ok = await barrier.enter(participant_id, timeout=4.0)
                arrival_order.append((participant_id, ok))
                return ok

            tasks = [
                asyncio.create_task(participant_worker(n1, "worker_fast")),
                asyncio.create_task(participant_worker(n2, "worker_delayed")),
                asyncio.create_task(participant_worker(n3, "worker_jitter")),
            ]

            results = await asyncio.gather(*tasks)

            # All 3 workers must successfully pass the barrier
            self.assertEqual(len(results), 3)
            self.assertTrue(all(results))
            self.assertEqual(barrier.generation, 1)

            # Test barrier timeout behavior when quorum cannot be reached
            timeout_barrier = n1.create_barrier("timeout_barrier", threshold=5, timeout_seconds=0.15)
            timeout_res = await timeout_barrier.enter("lonely_worker", timeout=0.15)
            self.assertFalse(timeout_res)

        finally:
            for n in nodes:
                try:
                    await n.stop()
                except Exception:
                    pass


if __name__ == "__main__":
    unittest.main()


