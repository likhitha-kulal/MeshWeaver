"""
MeshWeaver Data Models (Execution & Reliability Track - Person B / Likhitha)
Defines task payloads, result containers, and execution status dataclasses.
"""

from dataclasses import dataclass, field
from enum import Enum
import json
import time
from typing import Any, Dict, Optional, Union
import uuid


class TaskMessageType(str, Enum):
    """Message types for task execution stream."""
    TASK_EXECUTE = "TASK_EXECUTE"
    TASK_RESULT = "TASK_RESULT"
    ERROR = "ERROR"


@dataclass
class TaskResult:
    """
    Encapsulates the output or error of a remote task execution.
    """
    task_id: str
    success: bool
    result_bytes: Optional[bytes] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    traceback: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "success": self.success,
            "result_bytes": self.result_bytes.hex() if self.result_bytes else None,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "traceback": self.traceback,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskResult":
        res_bytes = bytes.fromhex(data["result_bytes"]) if data.get("result_bytes") else None
        return cls(
            task_id=data["task_id"],
            success=data["success"],
            result_bytes=res_bytes,
            error_type=data.get("error_type"),
            error_message=data.get("error_message"),
            traceback=data.get("traceback"),
        )
