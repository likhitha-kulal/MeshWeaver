"""
Unit tests for Write-Ahead Log (WAL) Engine, Crash Recovery Manager, and SnapshotDiskStore.
"""

import asyncio
import os
import shutil
import tempfile
import unittest

from meshweaver.models import (
    FsyncMode,
    LogEntry,
    RaftCommandType,
    StorageConfig,
    WALRecord,
    WALRecordType,
)
from meshweaver.raft_log import RaftLog, ReplicatedStateMachine
from meshweaver.storage import SnapshotDiskStore
from meshweaver.wal import CrashRecoveryManager, WALEngine, WALSegment


class TestWALStorage(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config = StorageConfig(
            data_dir=self.temp_dir,
            node_storage_id="test_node_wal",
            max_segment_size_bytes=500,  # Small size to trigger fast rotation
            fsync_mode=FsyncMode.OFF,
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_wal_engine_append_and_iter(self):
        engine = WALEngine(self.config)
        await engine.start()

        # Append records
        r1 = await engine.append(WALRecordType.ENTRY, {"key": "x", "val": 10})
        r2 = await engine.append(WALRecordType.ENTRY, {"key": "y", "val": 20})
        r3 = await engine.append(WALRecordType.COMMIT_MARKER, {"commit_index": 2})

        self.assertEqual(r1.seq_no, 1)
        self.assertEqual(r2.seq_no, 2)
        self.assertEqual(r3.seq_no, 3)

        # Iterate all records
        records = list(engine.iter_all_records())
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0].payload["key"], "x")
        self.assertEqual(records[2].record_type, WALRecordType.COMMIT_MARKER)

        metrics = engine.get_metrics()
        self.assertEqual(metrics["total_records_written"], 3)
        self.assertEqual(metrics["current_seq"], 3)

        await engine.stop()

    async def test_wal_segment_rotation_and_purge(self):
        engine = WALEngine(self.config)
        await engine.start()

        # Write enough records to trigger automatic rotation (> 500 bytes)
        for i in range(15):
            await engine.append(
                WALRecordType.ENTRY,
                {"index": i + 1, "term": 1, "cmd": "SET", "key": f"key_{i}", "val": f"val_{i}" * 5},
            )

        # Should have rotated across multiple segments
        self.assertTrue(len(engine.segments) >= 2)

        # Manually rotate
        await engine.rotate_segment()
        self.assertTrue(len(engine.segments) >= 3)

        # Purge older segments
        purged = engine.purge_segments_before(5)
        self.assertTrue(purged >= 1)

        await engine.stop()

    async def test_wal_corruption_recovery(self):
        engine = WALEngine(self.config)
        await engine.start()

        # Write 3 records
        await engine.append(WALRecordType.ENTRY, {"seq": 1, "k": "a"})
        await engine.append(WALRecordType.ENTRY, {"seq": 2, "k": "b"})
        await engine.append(WALRecordType.ENTRY, {"seq": 3, "k": "c"})
        await engine.stop()

        # Corrupt active segment file by appending malformed garbage line
        seg_file = engine.active_segment.file_path
        with open(seg_file, "a", encoding="utf-8") as f:
            f.write("CORRUPTED_GARBAGE_LINE_WITHOUT_JSON\n")

        # Reopen and iterate -> should yield first 3 valid records safely without crashing
        engine2 = WALEngine(self.config)
        await engine2.start()
        records = list(engine2.iter_all_records(verify_crc=True))
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0].payload["k"], "a")
        self.assertEqual(records[2].payload["k"], "c")
        await engine2.stop()

    async def test_crash_recovery_state_machine_replay(self):
        # 1. Start engine and write replicated entries
        engine = WALEngine(self.config)
        await engine.start()

        e1 = LogEntry(index=1, term=1, command_type=RaftCommandType.SET, key="name", value="Alice")
        e2 = LogEntry(index=2, term=1, command_type=RaftCommandType.INCREMENT, key="counter", value=5, extra_data={"delta": 5})
        e3 = LogEntry(index=3, term=1, command_type=RaftCommandType.SET, key="role", value="Admin")

        await engine.append(WALRecordType.ENTRY, {"entry": e1.to_dict(), "committed": True})
        await engine.append(WALRecordType.ENTRY, {"entry": e2.to_dict(), "committed": True})
        await engine.append(WALRecordType.ENTRY, {"entry": e3.to_dict(), "committed": True})
        await engine.append(WALRecordType.COMMIT_MARKER, {"commit_index": 3})

        await engine.stop()

        # 2. Simulate node reboot with blank state machine and RaftLog
        state_machine = ReplicatedStateMachine()
        raft_log = RaftLog()

        replayed, errors, last_seq = CrashRecoveryManager.recover(
            wal_engine=engine,
            state_machine=state_machine,
            raft_log=raft_log,
        )

        self.assertEqual(replayed, 4)
        self.assertEqual(errors, 0)
        self.assertEqual(last_seq, 4)

        # Verify state machine reconstructed identically
        self.assertEqual(state_machine.get("name"), "Alice")
        self.assertEqual(state_machine.get("counter"), 5)
        self.assertEqual(state_machine.get("role"), "Admin")
        self.assertEqual(raft_log.last_index, 3)
        self.assertEqual(raft_log.commit_index, 3)

    def test_snapshot_disk_store_save_load_and_cleanup(self):
        store = SnapshotDiskStore(self.config)

        data1 = {"state": {"counter": 100}, "jobs": {}, "locks": {}}
        path1 = store.save_snapshot(data1, last_index=10, last_term=1)
        self.assertTrue(os.path.exists(path1))

        data2 = {"state": {"counter": 200}, "jobs": {}, "locks": {}}
        path2 = store.save_snapshot(data2, last_index=20, last_term=2)
        self.assertTrue(os.path.exists(path2))

        # Load latest
        latest_data, idx, term = store.load_latest_snapshot()
        self.assertEqual(idx, 20)
        self.assertEqual(term, 2)
        self.assertEqual(latest_data["state"]["counter"], 200)

        # Test snapshot pruning
        data3 = {"state": {"counter": 300}}
        data4 = {"state": {"counter": 400}}
        data5 = {"state": {"counter": 500}}
        store.save_snapshot(data3, last_index=30, last_term=2)
        store.save_snapshot(data4, last_index=40, last_term=2)
        store.save_snapshot(data5, last_index=50, last_term=2)

        metrics = store.get_metrics()
        self.assertEqual(metrics["snapshot_count"], 3)  # max_snapshots_retained=3


if __name__ == "__main__":
    unittest.main()
