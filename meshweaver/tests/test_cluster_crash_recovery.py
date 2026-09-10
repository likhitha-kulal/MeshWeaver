"""
Integration test for cluster node crash, reboot, and WAL state machine recovery.
"""

import asyncio
import shutil
import tempfile
import unittest

from meshweaver.models import StorageConfig
from meshweaver.node import MeshNode


class TestClusterCrashRecoveryIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.mkdtemp()

    async def asyncTearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_node_crash_and_wal_state_reconstruction(self):
        storage_cfg = StorageConfig(
            data_dir=self.temp_dir,
            node_storage_id="crashed_node",
            enable_wal=True,
        )

        # 1. Start original node and commit state mutations
        node1 = MeshNode(
            host="127.0.0.1",
            udp_port=19850,
            tcp_port=19851,
            storage_config=storage_cfg,
        )
        await node1.start()

        # Perform transactions and state mutations
        async with await node1.begin_transaction() as tx:
            tx.set("system:status", "OPERATIONAL")
            tx.set("system:version", "v2.0.0")
            tx.increment("system:boot_count", delta=1)

        # Force sync WAL to disk
        node1.wal_engine.active_segment.sync()

        self.assertEqual(node1.state_get("system:status"), "OPERATIONAL")
        self.assertEqual(node1.state_get("system:boot_count"), 1)

        # 2. Simulate node crash by closing transport and stopping process
        await node1.stop()

        # 3. Reboot: Instantiate brand new MeshNode pointing to exact same disk directory
        node1_rebooted = MeshNode(
            host="127.0.0.1",
            udp_port=19860,
            tcp_port=19861,
            storage_config=storage_cfg,
        )
        await node1_rebooted.start()

        try:
            # 4. Verify all state was replayed and restored identically
            self.assertEqual(node1_rebooted.state_get("system:status"), "OPERATIONAL")
            self.assertEqual(node1_rebooted.state_get("system:version"), "v2.0.0")
            self.assertEqual(node1_rebooted.state_get("system:boot_count"), 1)

            wal_metrics = node1_rebooted.get_wal_metrics()
            self.assertTrue(wal_metrics["current_seq"] >= 1)

        finally:
            await node1_rebooted.stop()


if __name__ == "__main__":
    unittest.main()
