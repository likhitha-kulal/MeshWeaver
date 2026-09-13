"""
Unit and integration tests for LocalClusterRunner process supervisor (Week 4 Day 5).
"""

import asyncio
import shutil
import tempfile
import unittest

from meshweaver.cluster_runner import LocalClusterRunner
from meshweaver.models import ClusterConfig, ClusterTopology, NodeLifecycleState


class TestLocalClusterRunner(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.mkdtemp()

    async def asyncTearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_cluster_start_and_stop_lifecycle(self):
        cfg = ClusterConfig(
            cluster_name="TestMesh-1",
            node_count=3,
            topology=ClusterTopology.FULL_MESH,
            data_dir=self.temp_dir,
            auto_bootstrap=True,
        )
        runner = LocalClusterRunner(cfg)
        nodes = await runner.start()

        try:
            self.assertEqual(len(nodes), 3)
            self.assertTrue(runner.is_running)

            summary = runner.get_cluster_summary()
            self.assertEqual(summary["total_nodes"], 3)
            self.assertEqual(summary["healthy_nodes"], 3)
            self.assertEqual(summary["crashed_nodes"], 0)

            table = runner.format_status_table()
            self.assertIn("TestMesh-1", table)
            self.assertIn("node-1", table)
            self.assertIn("node-2", table)
            self.assertIn("node-3", table)
        finally:
            await runner.stop()
            self.assertFalse(runner.is_running)

    async def test_node_kill_and_reboot_cycle(self):
        cfg = ClusterConfig(
            cluster_name="TestMesh-Reboot",
            node_count=3,
            topology=ClusterTopology.FULL_MESH,
            data_dir=self.temp_dir,
            auto_bootstrap=True,
        )
        runner = LocalClusterRunner(cfg)
        await runner.start()

        try:
            node2 = runner.get_node("node-2")
            self.assertIsNotNone(node2)

            # Mutate state on node-2 via transaction
            async with await node2.begin_transaction() as tx:
                tx.set("persist_key", "reboot_resilient_value")
            if node2.wal_engine and node2.wal_engine.active_segment:
                node2.wal_engine.active_segment.sync()

            # Kill node-2 (simulated crash)
            kill_ok = await runner.kill_node("node-2", simulated_crash=True)
            self.assertTrue(kill_ok)
            self.assertIsNone(runner.get_node("node-2"))
            self.assertEqual(runner.processes["node-2"].state, NodeLifecycleState.CRASHED)

            summary = runner.get_cluster_summary()
            self.assertEqual(summary["crashed_nodes"], 1)

            # Cold reboot node-2
            rebooted = await runner.restart_node("node-2")
            self.assertIsNotNone(rebooted)
            self.assertEqual(runner.processes["node-2"].state, NodeLifecycleState.HEALTHY)
            self.assertEqual(runner.processes["node-2"].restart_count, 1)

            # Verify persisted state replayed from WAL
            self.assertEqual(rebooted.state_get("persist_key"), "reboot_resilient_value")

        finally:
            await runner.stop()

    async def test_cluster_election_trigger(self):
        cfg = ClusterConfig(
            cluster_name="TestMesh-Election",
            node_count=3,
            topology=ClusterTopology.FULL_MESH,
            data_dir=self.temp_dir,
            auto_bootstrap=True,
        )
        runner = LocalClusterRunner(cfg)
        await runner.start()

        try:
            leader = await runner.trigger_cluster_election("node-1")
            self.assertIsNotNone(leader)
            self.assertTrue(leader.is_leader)
            self.assertIsNotNone(runner.leader)
        finally:
            await runner.stop()


if __name__ == "__main__":
    unittest.main()
