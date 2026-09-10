"""
MeshWeaver Write-Ahead Log (WAL) Storage Engine.
Provides high-throughput append-only disk logging with CRC32 framing, segment rotation,
configurable fsync durability policies, corruption recovery, and state machine replay.
"""

import asyncio
import glob
import json
import logging
import os
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

from meshweaver.models import FsyncMode, StorageConfig, WALRecord, WALRecordType

logger = logging.getLogger("meshweaver.wal")


class WALSegment:
    """
    Manages an individual append-only WAL segment file on disk.
    Each record is framed as a single-line JSON string terminated by newline with CRC32 checksum.
    """

    def __init__(self, file_path: str, segment_id: int):
        self.file_path = file_path
        self.segment_id = segment_id
        self._file_handle = None
        self.record_count = 0
        self.start_seq = -1
        self.end_seq = -1
        self._open()

    def _open(self) -> None:
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        # Open in append + update mode
        self._file_handle = open(self.file_path, "a+", encoding="utf-8")
        self._scan_existing()

    def _scan_existing(self) -> None:
        """Scan existing segment file to establish record count and sequence numbers."""
        self._file_handle.seek(0)
        self.record_count = 0
        for line in self._file_handle:
            line_str = line.strip()
            if not line_str:
                continue
            try:
                rec = WALRecord.from_json_line(line_str)
                if self.start_seq == -1:
                    self.start_seq = rec.seq_no
                self.end_seq = rec.seq_no
                self.record_count += 1
            except Exception:
                break
        self._file_handle.seek(0, os.SEEK_END)

    @property
    def file_size(self) -> int:
        if os.path.exists(self.file_path):
            return os.path.getsize(self.file_path)
        return 0

    def append(self, record: WALRecord, fsync: bool = False) -> int:
        """Append record to segment and optionally trigger fsync."""
        line = record.to_json_line()
        self._file_handle.write(line)
        if self.start_seq == -1:
            self.start_seq = record.seq_no
        self.end_seq = record.seq_no
        self.record_count += 1

        if fsync:
            self.sync()
        else:
            self.flush()

        return len(line.encode("utf-8"))

    def flush(self) -> None:
        if self._file_handle and not self._file_handle.closed:
            self._file_handle.flush()

    def sync(self) -> None:
        """Flush user-space buffers and issue OS-level fsync barrier."""
        if self._file_handle and not self._file_handle.closed:
            self._file_handle.flush()
            try:
                os.fsync(self._file_handle.fileno())
            except (OSError, ValueError):
                pass

    def close(self) -> None:
        if self._file_handle and not self._file_handle.closed:
            self.sync()
            self._file_handle.close()

    def iter_records(self, verify_crc: bool = True) -> Iterator[WALRecord]:
        """Iterate over all valid records in this segment."""
        self.flush()
        if not os.path.exists(self.file_path):
            return

        with open(self.file_path, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                line_str = line.strip()
                if not line_str:
                    continue
                try:
                    rec = WALRecord.from_json_line(line_str)
                    if verify_crc and not rec.verify_crc32():
                        logger.warning(
                            f"CRC32 mismatch in segment {self.segment_id} line {line_no}. Stopping replay."
                        )
                        break
                    yield rec
                except Exception as e:
                    logger.warning(f"Corrupted record in segment {self.segment_id} line {line_no}: {e}")
                    break


class WALEngine:
    """
    Asynchronous Write-Ahead Log Storage Engine.
    Coordinates segment rotation, background fsync loops, and append operations.
    """

    def __init__(self, config: Optional[StorageConfig] = None):
        self.config = config or StorageConfig()
        self.wal_dir = os.path.join(self.config.data_dir, self.config.node_storage_id, "wal")
        self.current_seq: int = 0
        self.active_segment: Optional[WALSegment] = None
        self.segments: List[WALSegment] = []
        self._fsync_task: Optional[asyncio.Task] = None
        self._running: bool = False
        self._lock = asyncio.Lock()
        self.total_records_written: int = 0
        self.total_bytes_written: int = 0
        self.total_fsyncs: int = 0

    async def start(self) -> None:
        """Initialize storage directories and load or create active segment."""
        os.makedirs(self.wal_dir, exist_ok=True)
        self._discover_segments()
        if not self.segments:
            self._create_new_segment(1)
        else:
            self.active_segment = self.segments[-1]
            if self.active_segment.end_seq != -1:
                self.current_seq = self.active_segment.end_seq

        self._running = True
        if self.config.fsync_mode == FsyncMode.PERIODIC:
            self._fsync_task = asyncio.create_task(self._periodic_fsync_loop())

        logger.info(
            f"WALEngine initialized at {self.wal_dir} (current_seq={self.current_seq}, segments={len(self.segments)})"
        )

    async def stop(self) -> None:
        """Gracefully stop WALEngine, flush active buffers, and sync to disk."""
        self._running = False
        if self._fsync_task:
            self._fsync_task.cancel()
            try:
                await self._fsync_task
            except asyncio.CancelledError:
                pass

        async with self._lock:
            if self.active_segment:
                self.active_segment.close()

    def _discover_segments(self) -> None:
        """Scan directory for segment_*.wal files and load them in sorted order."""
        pattern = os.path.join(self.wal_dir, "segment_*.wal")
        files = sorted(glob.glob(pattern))
        self.segments = []
        for f in files:
            base = os.path.basename(f)
            try:
                seg_id = int(base.replace("segment_", "").replace(".wal", ""))
                seg = WALSegment(f, seg_id)
                self.segments.append(seg)
                if seg.end_seq > self.current_seq:
                    self.current_seq = seg.end_seq
            except Exception as e:
                logger.error(f"Failed loading WAL segment {f}: {e}")

    def _create_new_segment(self, segment_id: int) -> WALSegment:
        filename = f"segment_{segment_id:06d}.wal"
        file_path = os.path.join(self.wal_dir, filename)
        seg = WALSegment(file_path, segment_id)
        self.segments.append(seg)
        self.active_segment = seg
        return seg

    async def _periodic_fsync_loop(self) -> None:
        """Background periodic fsync task."""
        while self._running:
            try:
                await asyncio.sleep(self.config.fsync_interval_seconds)
                async with self._lock:
                    if self.active_segment:
                        self.active_segment.sync()
                        self.total_fsyncs += 1
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error during periodic fsync: {e}")

    async def append(
        self,
        record_type: WALRecordType,
        payload: Dict[str, Any],
        force_fsync: Optional[bool] = None,
    ) -> WALRecord:
        """
        Append a new record to the WAL.
        Assigns sequential sequence number, calculates CRC32 checksum, and writes to active segment.
        """
        if not self.config.enable_wal:
            self.current_seq += 1
            return WALRecord(seq_no=self.current_seq, record_type=record_type, payload=payload)

        async with self._lock:
            self.current_seq += 1
            record = WALRecord(
                seq_no=self.current_seq,
                record_type=record_type,
                payload=payload,
                timestamp=time.time(),
            )

            if not self.active_segment:
                self._create_new_segment(1)

            # Check segment rotation
            if self.active_segment.file_size >= self.config.max_segment_size_bytes:
                next_id = self.active_segment.segment_id + 1
                self.active_segment.close()
                self._create_new_segment(next_id)

            should_sync = False
            if force_fsync is not None:
                should_sync = force_fsync
            elif self.config.fsync_mode == FsyncMode.ALWAYS:
                should_sync = True

            bytes_written = self.active_segment.append(record, fsync=should_sync)
            self.total_records_written += 1
            self.total_bytes_written += bytes_written
            if should_sync:
                self.total_fsyncs += 1

            return record
