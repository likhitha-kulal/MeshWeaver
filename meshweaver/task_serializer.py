"""
MeshWeaver Task Serializer
Cloudpickle-based task serialization engine with SHA-256 integrity envelopes and async execution support.
"""

import asyncio
import inspect
import json
import sys
import traceback
from typing import Any, Callable, Dict, Tuple
import uuid

import cloudpickle
from meshweaver.models import TaskEnvelope, TaskResult


class RemoteExecutionError(Exception):
    """Raised when a remote task fails during execution."""

    def __init__(self, error_type: str, error_message: str, remote_traceback: str):
        self.error_type = error_type
        self.error_message = error_message
        self.remote_traceback = remote_traceback
        super().__init__(
            f"Remote task failed with {error_type}: {error_message}\n"
            f"--- Remote Traceback ---\n{remote_traceback}"
        )


class TaskSerializer:
    """
    Serializes, deserializes, and executes Python functions across distributed mesh nodes.
    """

    @staticmethod
    def serialize(func: Callable, *args: Any, **kwargs: Any) -> bytes:
        """
        Serialize a Python callable and its arguments.
        Wraps payload inside an integrity-checked TaskEnvelope.
        """
        if not callable(func):
            raise ValueError(f"Target {func} is not callable")

        task_data = {
            "func": func,
            "args": args,
            "kwargs": kwargs,
        }
        payload_bytes = cloudpickle.dumps(task_data)
        envelope = TaskEnvelope.wrap(payload_bytes)
        return json.dumps(envelope.to_dict()).encode("utf-8")

    @staticmethod
    def deserialize(payload: bytes) -> Tuple[Callable, Tuple[Any, ...], Dict[str, Any]]:
        """
        Deserialize payload into (func, args, kwargs).
        Verifies TaskEnvelope hash before passing to cloudpickle.
        """
        try:
            try:
                json_str = payload.decode("utf-8")
                envelope_dict = json.loads(json_str)
                if isinstance(envelope_dict, dict) and "sha256" in envelope_dict and "payload" in envelope_dict:
                    envelope = TaskEnvelope.from_dict(envelope_dict)
                    if not envelope.verify():
                        raise ValueError("TaskEnvelope integrity check failed: hash mismatch")
                    task_bytes = envelope.payload
                else:
                    task_bytes = payload
            except (UnicodeDecodeError, json.JSONDecodeError):
                task_bytes = payload

            task_data = cloudpickle.loads(task_bytes)
            if not isinstance(task_data, dict) or "func" not in task_data:
                raise ValueError("Deserialized payload is not a valid task specification")
            return task_data["func"], task_data.get("args", ()), task_data.get("kwargs", {})
        except Exception as e:
            raise ValueError(f"Failed to deserialize task payload: {e}") from e

    @classmethod
    async def execute_task(cls, payload: bytes, task_id: str = "") -> TaskResult:
        """
        Execute task safely in the local event loop.
        Supports both standard functions and async coroutine functions.
        """
        if not task_id:
            task_id = str(uuid.uuid4())

        try:
            func, args, kwargs = cls.deserialize(payload)

            if inspect.iscoroutinefunction(func):
                raw_result = await func(*args, **kwargs)
            else:
                raw_result = func(*args, **kwargs)

            result_bytes = cloudpickle.dumps(raw_result)
            return TaskResult(
                task_id=task_id,
                success=True,
                result_bytes=result_bytes,
            )

        except Exception as err:
            exc_type, exc_val, exc_tb = sys.exc_info()
            tb_str = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            err_msg = str(err)
            err_type = "IntegrityError" if "hash mismatch" in err_msg.lower() else type(err).__name__

            return TaskResult(
                task_id=task_id,
                success=False,
                error_type=err_type,
                error_message=err_msg,
                traceback=tb_str,
            )

    @staticmethod
    def unpack_result(task_result: TaskResult) -> Any:
        """Unpack execution result or raise RemoteExecutionError."""
        if task_result.success:
            if task_result.result_bytes is None:
                return None
            return cloudpickle.loads(task_result.result_bytes)
        else:
            raise RemoteExecutionError(
                error_type=task_result.error_type or "UnknownError",
                error_message=task_result.error_message or "No error message provided",
                remote_traceback=task_result.traceback or "No traceback available",
            )
