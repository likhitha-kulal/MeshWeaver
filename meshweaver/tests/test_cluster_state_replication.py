"""
Integration tests for multi-node MeshWeaver cluster state replication and distributed locking.
"""

import asyncio
import unittest

from meshweaver.models import RaftCommandType
from meshweaver.node import MeshNode


class TestClusterStateReplicationIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_cluster_state_replication_and_distributed_lock(self):
        # Spin up 3-node cluster on isolated test ports
        n1 = MeshNode(host="127.0.0.1", udp_port=19600, tcp_port=19601)
        n2 = MeshNode(host="127.0.0.1", udp_port=19610, tcp_port=19611)
        n3 = MeshNode(host="127.0.0.1", udp_port=19620, tcp_port=19621)

        nodes = [n1, n2, n3]
        for n in nodes:
            await n.start()
            n.leader_election.config.min_election_timeout = 0.150
            n.leader_election.config.max_election_timeout = 0.300
            n.leader_election.config.heartbeat_interval = 0.040

        try:
            # Bootstrap cluster
            await n2.bootstrap([("127.0.0.1", 19600)])
            await n3.bootstrap([("127.0.0.1", 19600)])
            await n1.bootstrap([("127.0.0.1", 19610)])
            await asyncio.sleep(0.15)

            # Elect n1 as leader
            await n1.trigger_election()
            await asyncio.sleep(0.4)

            # Leader (n1) sets key
            val = await n1.state_set("cluster_name", "HyperMesh-1")
            self.assertEqual(val, "HyperMesh-1")
            self.assertEqual(n1.state_get("cluster_name"), "HyperMesh-1")

            # Increment shared counter
            c1 = await n1.state_increment("task_counter", delta=10)
            self.assertEqual(c1, 10)
            self.assertEqual(n1.state_get("task_counter"), 10)

            # Atomic Compare-And-Swap (CAS)
            cas_ok = await n1.state_cas("cluster_name", expected="HyperMesh-1", new_value="HyperMesh-Alpha")
            self.assertTrue(cas_ok)
            self.assertEqual(n1.state_get("cluster_name"), "HyperMesh-Alpha")

            # Distributed Lock Acquisition
            lock_res1 = await n1.acquire_lock("db_write_mutex", ttl_seconds=10.0)
            self.assertTrue(lock_res1.acquired)
            self.assertEqual(lock_res1.fencing_token, 1)

            # Release Lock
            rel_ok = await n1.release_lock("db_write_mutex", fencing_token=1)
            self.assertTrue(rel_ok)

            # Re-acquire lock -> receives monotonic incremented fencing token
            lock_res2 = await n1.acquire_lock("db_write_mutex", ttl_seconds=10.0)
            self.assertTrue(lock_res2.acquired)
            self.assertEqual(lock_res2.fencing_token, 2)

            # Verify metrics
            metrics = n1.get_raft_metrics()
            self.assertTrue(metrics.total_proposals >= 5)
            self.assertTrue(metrics.state_keys_count >= 2)

        finally:
            for n in nodes:
                try:
                    await n.stop()
                except Exception:
                    pass


if __name__ == "__main__":
    unittest.main()
