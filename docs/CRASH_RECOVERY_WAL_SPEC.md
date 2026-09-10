# Write-Ahead Log (WAL) & Crash Recovery Storage Specification

## 1. Overview
MeshWeaver Persistent Storage Engine (`meshweaver.wal` & `meshweaver.storage`) provides durability and crash recovery for Raft consensus logs, replicated state machines, and 2PC distributed transactions.

---

## 2. WAL Binary Frame Format & CRC32 Framing

Every record appended to an active `.wal` segment file follows a structured binary framing layout:

```
+----------------+----------------+----------------+-------------------+----------------+
|  Magic Bytes   | Sequence (u64) | Payload Length |    JSON Payload   |   CRC32 (u32)  |
|   (4 Bytes)    |   (8 Bytes)    |   (4 Bytes)    |     (N Bytes)     |   (4 Bytes)    |
|   0x57414C31   |  Monotonic Seq | Big-Endian Len | UTF-8 Serialized  | IEEE 802.3 CRC |
+----------------+----------------+----------------+-------------------+----------------+
```

### Corruption Detection Invariant:
$$\text{CRC32}(\text{Payload}) == \text{Stored CRC32}$$
If checksum validation fails during scan, recovery truncates to the last known healthy sequence boundary and warns of bit-rot / torn write.

---

## 3. Fsync Durability Policies (`FsyncMode`)

| Policy | Behavior | Durability Level | Throughput | Use Case |
| :--- | :--- | :--- | :--- | :--- |
| `ALWAYS` | `os.fsync()` on every single append | Maximum | ~1,000 writes/sec | Financial transactions, ACID state |
| `PERIODIC` | Background task flushes disk buffer every $T$ ms | Balanced (loss window $\le T$) | ~50,000 writes/sec | Default compute mesh consensus |
| `BATCH` | `os.fsync()` after $N$ batched mutations | High Throughput | ~80,000 writes/sec | High-frequency MapReduce pipelines |
| `OFF` | OS page cache handles disk flushing | Memory-Safe | ~150,000 writes/sec | Ephemeral testing, in-memory caches |

---

## 4. Segment Rotation & Log Compaction Lifecycle

```
[ Active WAL Segment (seq_000001_active.wal) ]
                  │
          Size >= max_segment_size_bytes?
                  │
                  ├── YES ──► Close & rename to seq_000001_000500.wal
                  │           Create seq_000501_active.wal
                  │
        Raft Snapshot Triggered?
                  │
                  ├── YES ──► SnapshotDiskStore writes state_snapshot_<idx>.json.gz
                  │           Append SNAPSHOT_POINTER record to WAL
                  │           Prune WAL segments older than retention threshold
```

---

## 5. Crash Recovery Protocol (`CrashRecoveryManager`)

On node boot or restart after ungraceful crash:
1. **Segment Discovery**: Identify all `.wal` segments ordered by initial sequence index.
2. **Sequential Log Replay**:
   - Verify Magic Header and CRC32 framing.
   - Reconstruct `RaftLog` entries in order.
   - Replay committed `TxRecord` and `SET` operations into in-memory `ReplicatedStateMachine`.
   - Restore monotonic transaction version counters and sequence offsets.
3. **Snapshot Merging**: If compacted snapshot exists, load base snapshot state first, then replay delta WAL records starting from `snapshot_last_applied_index + 1`.
4. **Resumption**: Expose online node with 100% state consistency and zero data loss.
