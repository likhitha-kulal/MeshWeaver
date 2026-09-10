"""
Unit tests for Distributed 2-Phase Commit (2PC) Transaction Coordinator,
OCC version fencing, lock conflicts, and rollback.
"""

import asyncio
import os
import shutil
import tempfile
import unittest

from meshweaver.models import (
    FsyncMode,
    StorageConfig,
    TxIsolationLevel,
    TxStatus,
)
from meshweaver.raft_log import ReplicatedStateMachine
from meshweaver.transactions import TransactionCoordinator
from meshweaver.wal import WALEngine


class TestTransactions(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.storage_config = StorageConfig(
            data_dir=self.temp_dir,
            node_storage_id="tx_test_node",
            fsync_mode=FsyncMode.OFF,
        )
        self.wal_engine = WALEngine(self.storage_config)
        self.state_machine = ReplicatedStateMachine()
        self.coordinator = TransactionCoordinator(
            node_id="coord-node-1",
            wal_engine=self.wal_engine,
            state_machine=self.state_machine,
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def asyncSetUp(self):
        await self.wal_engine.start()

    async def asyncTearDown(self):
        await self.wal_engine.stop()

    async def test_successful_2pc_transaction_commit(self):
        # 1. Begin transaction
        tx = await self.coordinator.begin_transaction(
            isolation_level=TxIsolationLevel.SERIALIZABLE,
            timeout_seconds=5.0,
        )
        self.assertEqual(tx.coordinator.node_id, "coord-node-1")

        # 2. Buffer operations
        tx.set("user:100", {"name": "Bob", "balance": 1000})
        tx.increment("total_users", delta=1)

        # 3. Commit
        ok = await tx.commit()
        self.assertTrue(ok)

        # 4. Verify state machine updated atomically
        self.assertEqual(self.state_machine.get("user:100"), {"name": "Bob", "balance": 1000})
        self.assertEqual(self.state_machine.get("total_users"), 1)

        # 5. Check transaction record status and metrics
        tx_rec = self.coordinator.get_transaction(tx.tx_id)
        self.assertIsNotNone(tx_rec)
        self.assertEqual(tx_rec.status, TxStatus.COMMITTED)
        self.assertIsNotNone(tx_rec.completed_at)

        metrics = self.coordinator.get_metrics()
        self.assertEqual(metrics["total_committed"], 1)
        self.assertEqual(metrics["locked_keys_count"], 0)

    async def test_transaction_context_manager_auto_commit(self):
        async with await self.coordinator.begin_transaction() as tx:
            tx.set("order:999", "Pending")
            tx.increment("order_count", delta=5)

        self.assertEqual(self.state_machine.get("order:999"), "Pending")
        self.assertEqual(self.state_machine.get("order_count"), 5)

    async def test_transaction_context_manager_exception_rollback(self):
        try:
            async with await self.coordinator.begin_transaction() as tx:
                tx.set("temp_key", "secret")
                raise ValueError("Simulated pipeline error")
        except ValueError:
            pass

        # Should not be in state machine
        self.assertFalse(self.state_machine.contains("temp_key"))
        self.assertEqual(self.coordinator.get_metrics()["total_aborted"], 1)

    async def test_lock_conflict_between_concurrent_transactions(self):
        tx1 = await self.coordinator.begin_transaction()
        tx2 = await self.coordinator.begin_transaction()

        tx1.set("shared_account", 500)
        tx2.set("shared_account", 800)

        # Prepare tx1 -> acquires lock on "shared_account"
        res1 = await self.coordinator.prepare_transaction(tx1.tx_id, tx1)
        self.assertTrue(res1.vote_yes)

        # Prepare tx2 -> should fail with lock conflict!
        res2 = await self.coordinator.prepare_transaction(tx2.tx_id, tx2)
        self.assertFalse(res2.vote_yes)
        self.assertIn("Lock conflict", res2.error_message)

        # Commit tx1 -> releases lock
        ok1 = await tx1.commit()
        self.assertTrue(ok1)
        self.assertEqual(self.state_machine.get("shared_account"), 500)

        # Now tx2 can be prepared and committed
        ok2 = await tx2.commit()
        self.assertTrue(ok2)
        self.assertEqual(self.state_machine.get("shared_account"), 800)

    async def test_occ_version_mismatch_detection(self):
        # 1. Commit initial value -> version becomes 1
        tx1 = await self.coordinator.begin_transaction()
        tx1.set("product:1", {"price": 50})
        await tx1.commit()

        curr_ver = self.coordinator._key_versions.get("product:1")
        self.assertEqual(curr_ver, 1)

        # 2. Start tx2 expecting version 0 -> should conflict on OCC check
        tx2 = await self.coordinator.begin_transaction()
        tx2.set("product:1", {"price": 60}, expected_version=0)
        ok2 = await tx2.commit()
        self.assertFalse(ok2)

        # Value should remain 50
        self.assertEqual(self.state_machine.get("product:1")["price"], 50)

        # 3. Start tx3 with correct expected version 1 -> should succeed
        tx3 = await self.coordinator.begin_transaction()
        tx3.set("product:1", {"price": 75}, expected_version=1)
        ok3 = await tx3.commit()
        self.assertTrue(ok3)
        self.assertEqual(self.state_machine.get("product:1")["price"], 75)

    async def test_transaction_timeout_expiration(self):
        tx = await self.coordinator.begin_transaction(timeout_seconds=0.05)
        tx.set("timeout_key", "val")
        await asyncio.sleep(0.08)

        # Commit after timeout should fail
        ok = await tx.commit()
        self.assertFalse(ok)
        self.assertFalse(self.state_machine.contains("timeout_key"))


if __name__ == "__main__":
    unittest.main()
