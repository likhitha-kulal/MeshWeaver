"""
Integration tests for multi-node MeshWeaver cluster consensus job orchestration and snapshot transfer.
"""

import asyncio
import unittest

from meshweaver.models import ConsensusJobStatus
from meshweaver.node import MeshNode


def task_cube(x: int) -> int:
    return x * x * x


class TestClusterOrchestratorIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_multi_node_consensus_job_orchestration(self):
        # Spin up 3-node cluster
        n1 = MeshNode(host="127.0.0.1", udp_port=19700, tcp_port=19701)
        n2 = MeshNode(host="127.0.0.1", udp_port=19710, tcp_port=19711)
        n3 = MeshNode(host="127.0.0.1", udp_port=19720, tcp_port=19721)

        nodes = [n1, n2, n3]
        for n in nodes:
            await n.start()
            n.leader_election.config.min_election_timeout = 0.150
            n.leader_election.config.max_election_timeout = 0.300
            n.leader_election.config.heartbeat_interval = 0.040

        try:
            # Bootstrap cluster
            await n2.bootstrap([("127.0.0.1", 19700)])
            await n3.bootstrap([("127.0.0.1", 19700)])
            await n1.bootstrap([("127.0.0.1", 19710)])
            await asyncio.sleep(0.15)

            # Elect n1 as leader
            await n1.trigger_election()
            await asyncio.sleep(0.4)

            # Submit consensus job to leader n1
            job_id = await n1.submit_consensus_job(task_cube, 4, priority=1)
            self.assertIsNotNone(job_id)

            # Await execution across the mesh
            result = await n1.await_consensus_job(job_id, poll_interval=0.05, timeout=5.0)
            self.assertEqual(result, 64)

            # Verify job recorded in state machine
            job = n1.get_consensus_job(job_id)
            self.assertIsNotNone(job)
            self.assertEqual(job.status, ConsensusJobStatus.COMPLETED)

            # Test Snapshot compaction on leader
            snap = n1.create_cluster_snapshot()
            self.assertIn("jobs", snap["data"])
            self.assertIn(job_id, snap["data"]["jobs"])

            # Test membership reconfig
            members = await n1.reconfigure_membership("127.0.0.1:19730", action="ADD")
            self.assertIn("127.0.0.1:19730", members)

            # Verify orchestrator metrics
            orch_metrics = n1.get_orchestrator_metrics()
            self.assertTrue(orch_metrics.total_submitted >= 1)
            self.assertTrue(orch_metrics.total_completed >= 1)

        finally:
            for n in nodes:
                try:
                    await n.stop()
                except Exception:
                    pass


if __name__ == "__main__":
    unittest.main()
