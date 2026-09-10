"""
Integration tests for multi-node MeshWeaver cluster distributed 2PC transactions.
"""

import asyncio
import shutil
import tempfile
import unittest

from meshweaver.models import StorageConfig, TxIsolationLevel, TxStatus
from meshweaver.node import MeshNode


class TestClusterTransactionsIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.mkdtemp()

    async def asyncTearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_multi_node_cluster_2pc_transaction_lifecycle(self):
        cfg1 = StorageConfig(data_dir=self.temp_dir, node_storage_id="node1")
        cfg2 = StorageConfig(data_dir=self.temp_dir, node_storage_id="node2")
        cfg3 = StorageConfig(data_dir=self.temp_dir, node_storage_id="node3")

        n1 = MeshNode(host="127.0.0.1", udp_port=19800, tcp_port=19801, storage_config=cfg1)
        n2 = MeshNode(host="127.0.0.1", udp_port=19810, tcp_port=19811, storage_config=cfg2)
        n3 = MeshNode(host="127.0.0.1", udp_port=19820, tcp_port=19821, storage_config=cfg3)

        nodes = [n1, n2, n3]
        for n in nodes:
            await n.start()

        try:
            # Bootstrap cluster
            await n2.bootstrap([("127.0.0.1", 19800)])
            await n3.bootstrap([("127.0.0.1", 19800)])
            await asyncio.sleep(0.15)

            # 1. Execute distributed 2PC transaction on node 1
            async with await n1.begin_transaction(isolation_level=TxIsolationLevel.SERIALIZABLE) as tx:
                tx.set("ledger:alice", 1000)
                tx.set("ledger:bob", 500)
                tx.increment("ledger:transfer_count", delta=1)

            # Verify committed on local state machine
            self.assertEqual(n1.state_get("ledger:alice"), 1000)
            self.assertEqual(n1.state_get("ledger:bob"), 500)
            self.assertEqual(n1.state_get("ledger:transfer_count"), 1)

            # 2. Test concurrent transaction conflict & rollback
            tx_a = await n1.begin_transaction()
            tx_b = await n1.begin_transaction()

            tx_a.set("locked_account", 100)
            tx_b.set("locked_account", 200)

            # Prepare tx_a -> locks key
            res_a = await n1.transaction_coordinator.prepare_transaction(tx_a.tx_id, tx_a)
            self.assertTrue(res_a.vote_yes)

            # Prepare tx_b -> conflicts and fails
            res_b = await n1.transaction_coordinator.prepare_transaction(tx_b.tx_id, tx_b)
            self.assertFalse(res_b.vote_yes)

            # Rollback tx_b
            await tx_b.rollback(reason="Lock conflict")
            self.assertEqual(tx_b.coordinator.get_transaction(tx_b.tx_id).status, TxStatus.ABORTED)

            # Commit tx_a
            await tx_a.commit()
            self.assertEqual(n1.state_get("locked_account"), 100)

            # Verify transaction telemetry
            tx_metrics = n1.get_transaction_metrics()
            self.assertTrue(tx_metrics["total_committed"] >= 2)
            self.assertTrue(tx_metrics["total_aborted"] >= 1)

        finally:
            for n in nodes:
                try:
                    await n.stop()
                except Exception:
                    pass


if __name__ == "__main__":
    unittest.main()
