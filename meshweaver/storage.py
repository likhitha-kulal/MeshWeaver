"""
MeshWeaver Storage Engine & Snapshot Disk Persistence.
Provides snapshot disk serialization, SHA-256 verification, and point-in-time state checkpointing.
"""

import glob
import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from meshweaver.models import StorageConfig

logger = logging.getLogger("meshweaver.storage")


class SnapshotDiskStore:
    """
    Manages point-in-time serialized state machine snapshots on disk.
    Ensures safe atomic writes with SHA-256 verification and automatic retention pruning.
    """

    def __init__(self, config: Optional[StorageConfig] = None):
        self.config = config or StorageConfig()
        self.snapshot_dir = os.path.join(
            self.config.data_dir, self.config.node_storage_id, "snapshots"
        )
        os.makedirs(self.snapshot_dir, exist_ok=True)
        self.total_snapshots_saved: int = 0
        self.total_snapshots_loaded: int = 0

    def save_snapshot(
        self,
        snapshot_data: Dict[str, Any],
        last_index: int,
        last_term: int,
    ) -> str:
        """
        Save state machine snapshot atomically to disk.
        Writes payload, metadata, and SHA-256 checksum to temporary file before atomic rename.
        """
        os.makedirs(self.snapshot_dir, exist_ok=True)
        filename = f"snapshot_{last_index:08d}_{last_term:04d}.snap"
        target_path = os.path.join(self.snapshot_dir, filename)
        temp_path = target_path + f".tmp.{int(time.time() * 1000)}"

        envelope = {
            "last_index": last_index,
            "last_term": last_term,
            "created_at": time.time(),
            "data": snapshot_data,
        }
        raw_json = json.dumps(envelope, indent=2)
        checksum = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

        final_payload = {
            "checksum_sha256": checksum,
            "envelope": envelope,
        }

        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(final_payload, f, indent=2)
            f.flush()
            os.fsync(f.fileno())

        # Atomic replace
        if os.path.exists(target_path):
            os.remove(target_path)
        os.rename(temp_path, target_path)

        self.total_snapshots_saved += 1
        logger.info(
            f"Saved snapshot to {target_path} (index={last_index}, term={last_term}, sha256={checksum[:8]}...)"
        )

        self.cleanup_old_snapshots(self.config.max_snapshots_retained)
        return target_path

    def load_latest_snapshot(self) -> Optional[Tuple[Dict[str, Any], int, int]]:
        """
        Discover and load the most recent valid snapshot from disk.
        Verifies SHA-256 checksum integrity. Returns (data, last_index, last_term) or None.
        """
        if not os.path.exists(self.snapshot_dir):
            return None

        pattern = os.path.join(self.snapshot_dir, "snapshot_*.snap")
        files = sorted(glob.glob(pattern), reverse=True)

        for file_path in files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = json.load(f)

                expected_sha = content.get("checksum_sha256")
                envelope = content.get("envelope", {})
                raw_envelope = json.dumps(envelope, indent=2)
                computed_sha = hashlib.sha256(raw_envelope.encode("utf-8")).hexdigest()

                if expected_sha and expected_sha != computed_sha:
                    logger.warning(f"Snapshot checksum mismatch in {file_path}. Skipping.")
                    continue

                last_index = int(envelope.get("last_index", 0))
                last_term = int(envelope.get("last_term", 0))
                data = envelope.get("data", {})

                self.total_snapshots_loaded += 1
                logger.info(
                    f"Successfully loaded snapshot {file_path} (index={last_index}, term={last_term})"
                )
                return data, last_index, last_term

            except Exception as e:
                logger.error(f"Error reading snapshot file {file_path}: {e}")

        return None

    def cleanup_old_snapshots(self, keep: int = 3) -> int:
        """Prune older snapshot files, keeping the latest N."""
        pattern = os.path.join(self.snapshot_dir, "snapshot_*.snap")
        files = sorted(glob.glob(pattern), reverse=True)
        removed = 0
        if len(files) > keep:
            for old_file in files[keep:]:
                try:
                    os.remove(old_file)
                    removed += 1
                    logger.info(f"Cleaned up stale snapshot {old_file}")
                except Exception as e:
                    logger.warning(f"Failed to remove stale snapshot {old_file}: {e}")
        return removed

    def get_metrics(self) -> Dict[str, Any]:
        """Return operational telemetry for snapshot store."""
        pattern = os.path.join(self.snapshot_dir, "snapshot_*.snap")
        count = len(glob.glob(pattern))
        return {
            "snapshot_dir": self.snapshot_dir,
            "snapshot_count": count,
            "total_snapshots_saved": self.total_snapshots_saved,
            "total_snapshots_loaded": self.total_snapshots_loaded,
        }
