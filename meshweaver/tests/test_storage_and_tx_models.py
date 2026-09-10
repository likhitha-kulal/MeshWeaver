"""
Unit tests for Week 4 Day 4 data models:
WAL records, StorageConfig, 2PC transactions, barriers, semaphores, and load shedder metrics.
"""

import time
import unittest

from meshweaver.models import (
    BackpressureStatus,
    BarrierState,
    DistributedBarrierSpec,
    DistributedSemaphoreSpec,
    FsyncMode,
    LoadShedderMetrics,
    MessageType,
    RaftCommandType,
    StorageConfig,
    TokenBucketConfig,
    TxIsolationLevel,
    TxOperation,
    TxOperationType,
    TxPrepareResult,
    TxRecord,
    TxStatus,
    WALRecord,
    WALRecordType,
)


class TestStorageAndTxModels(unittest.TestCase):
    def test_wal_record_crc32_and_serialization(self):
        rec = WALRecord(
            seq_no=1,
            record_type=WALRecordType.ENTRY,
            payload={"index": 10, "term": 2, "cmd": "SET", "key": "k1", "val": "v1"},
            timestamp=1700000000.0,
        )
        self.assertTrue(rec.verify_crc32())
        self.assertNotEqual(rec.crc32, 0)

        # JSON line serialization
        line = rec.to_json_line()
        self.assertTrue(line.endswith("\n"))

        deserialized = WALRecord.from_json_line(line)
        self.assertEqual(deserialized.seq_no, 1)
        self.assertEqual(deserialized.record_type, WALRecordType.ENTRY)
        self.assertEqual(deserialized.payload["key"], "k1")
        self.assertTrue(deserialized.verify_crc32())

        # Tampering corrupts CRC32
        deserialized.payload["val"] = "tampered"
        self.assertFalse(deserialized.verify_crc32())

    def test_storage_config_serialization(self):
        cfg = StorageConfig(
            data_dir=".custom_db",
            node_storage_id="node_abc",
            fsync_mode=FsyncMode.ALWAYS,
            fsync_interval_seconds=0.1,
            enable_wal=True,
        )
        d = cfg.to_dict()
        self.assertEqual(d["fsync_mode"], "ALWAYS")
        self.assertEqual(d["data_dir"], ".custom_db")

        reconstructed = StorageConfig.from_dict(d)
        self.assertEqual(reconstructed.fsync_mode, FsyncMode.ALWAYS)
        self.assertEqual(reconstructed.node_storage_id, "node_abc")
        self.assertTrue(reconstructed.enable_wal)

    def test_tx_operation_and_record_lifecycle(self):
        op1 = TxOperation(op_type=TxOperationType.SET, key="account_A", value=500, expected_version=1)
        op2 = TxOperation(op_type=TxOperationType.INCREMENT, key="tx_counter", delta=1)

        tx = TxRecord(
            tx_id="tx-1001",
            coordinator_id="node-leader-1",
            status=TxStatus.ACTIVE,
            isolation_level=TxIsolationLevel.SERIALIZABLE,
            operations=[op1, op2],
            read_set={"account_A": 1000},
            write_set={"account_A": 500},
            participants=["node-1", "node-2"],
            timeout_seconds=5.0,
        )

        d = tx.to_dict()
        self.assertEqual(d["status"], "ACTIVE")
        self.assertEqual(len(d["operations"]), 2)

        reconstructed = TxRecord.from_dict(d)
        self.assertEqual(reconstructed.tx_id, "tx-1001")
        self.assertEqual(len(reconstructed.operations), 2)
        self.assertEqual(reconstructed.operations[0].op_type, TxOperationType.SET)
        self.assertFalse(reconstructed.is_expired(now=time.time()))

        # Expired check
        self.assertTrue(reconstructed.is_expired(now=time.time() + 10.0))

    def test_tx_prepare_result(self):
        res = TxPrepareResult(
            tx_id="tx-1002",
            participant_id="node-2",
            vote_yes=True,
            fencing_tokens={"account_A": 42},
        )
        d = res.to_dict()
        reconstructed = TxPrepareResult.from_dict(d)
        self.assertTrue(reconstructed.vote_yes)
        self.assertEqual(reconstructed.fencing_tokens["account_A"], 42)

    def test_distributed_barrier_spec(self):
        b = DistributedBarrierSpec(
            barrier_id="stage-1-barrier",
            threshold=3,
            parties=["node-1", "node-2"],
            state=BarrierState.WAITING,
            timeout_seconds=2.0,
        )
        self.assertEqual(len(b.parties), 2)
        self.assertFalse(b.is_expired(now=time.time()))
        self.assertTrue(b.is_expired(now=time.time() + 5.0))

        d = b.to_dict()
        reconstructed = DistributedBarrierSpec.from_dict(d)
        self.assertEqual(reconstructed.threshold, 3)
        self.assertEqual(reconstructed.state, BarrierState.WAITING)

    def test_distributed_semaphore_spec(self):
        sem = DistributedSemaphoreSpec(
            semaphore_id="gpu_cluster_sem",
            total_permits=4,
            available_permits=2,
            holders={"worker-1": time.time() + 100.0, "worker-2": time.time() - 10.0},
        )
        self.assertEqual(sem.available_permits, 2)
        reclaimed = sem.cleanup_expired_leases(now=time.time())
        self.assertEqual(reclaimed, 1)
        self.assertEqual(sem.available_permits, 3)
        self.assertNotIn("worker-2", sem.holders)

        d = sem.to_dict()
        reconstructed = DistributedSemaphoreSpec.from_dict(d)
        self.assertEqual(reconstructed.available_permits, 3)

    def test_load_shedder_metrics_and_token_bucket(self):
        tb_cfg = TokenBucketConfig(capacity=200.0, refill_rate=80.0, min_refill_rate=10.0)
        d = tb_cfg.to_dict()
        tb_rec = TokenBucketConfig.from_dict(d)
        self.assertEqual(tb_rec.capacity, 200.0)

        metrics = LoadShedderMetrics(
            cpu_percent=72.5,
            ram_percent=60.0,
            composite_watermark=0.68,
            status=BackpressureStatus.MODERATE,
            total_admitted=150,
            total_shed=5,
            effective_rate=45.0,
        )
        m_dict = metrics.to_dict()
        m_rec = LoadShedderMetrics.from_dict(m_dict)
        self.assertEqual(m_rec.status, BackpressureStatus.MODERATE)
        self.assertEqual(m_rec.total_admitted, 150)
        self.assertEqual(m_rec.total_shed, 5)

    def test_new_message_and_raft_enums(self):
        self.assertIn(MessageType.TX_PREPARE_REQUEST, MessageType)
        self.assertIn(MessageType.TX_COMMIT_RESPONSE, MessageType)
        self.assertIn(MessageType.BARRIER_SYNC_REQUEST, MessageType)
        self.assertIn(RaftCommandType.TX_PREPARE, RaftCommandType)
        self.assertIn(RaftCommandType.BARRIER_ENTER, RaftCommandType)
        self.assertIn(RaftCommandType.SEMAPHORE_ACQUIRE, RaftCommandType)


if __name__ == "__main__":
    unittest.main()
