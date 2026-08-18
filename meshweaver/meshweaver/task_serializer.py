"""
MeshWeaver Task Serializer
Handles cloudpickle serialization/deserialization of arbitrary Python functions,
arguments, execution results, and remote error handling.
"""

import asyncio
import inspect
import sys
import traceback
from typing import Any, Callable, Dict, Tuple
import uuid

import cloudpickle
from meshweaver.models import TaskResult, TaskEnvelope


class RemoteExecutionError(Exception):
    """Raised when a remotely executed task raises an unhandled exception."""

    def __init__(self, error_type: str, error_message: str, remote_traceback: str):
        self.error_type = error_type
        self.error_message = error_message
        self.remote_traceback = remote_traceback
        super().__init__(
            f"Remote task execution failed with {error_type}: {error_message}\n"
            f"--- Remote Traceback ---\n{remote_traceback}"
        )


class TaskSerializer:
    """
    Utility class for serializing, deserializing, executing Python tasks,
    and packing/unpacking results safely across node boundaries.
    """

    @staticmethod
    def serialize(func: Callable, *args: Any, **kwargs: Any) -> bytes:
        """
        Serialize a Python function along with its positional and keyword arguments.
        Wraps the cloudpickled payload in a TaskEnvelope with SHA-256 integrity hash.
        Returns the envelope as JSON-encoded bytes.
        """
        if not callable(func):
            raise ValueError(f"Object {func} is not callable")

        task_data = {
            "func": func,
            "args": args,
            "kwargs": kwargs,
        }
        payload_bytes = cloudpickle.dumps(task_data)
        
        # Wrap payload in TaskEnvelope with hash
        envelope = TaskEnvelope.wrap(payload_bytes)
        
        # Serialize envelope to JSON bytes
        import json
        envelope_dict = envelope.to_dict()
        return json.dumps(envelope_dict).encode("utf-8")

    @staticmethod
    def deserialize(payload: bytes) -> Tuple[Callable, Tuple[Any, ...], Dict[str, Any]]:
        """
        Deserialize binary payload into (func, args, kwargs).
        First unwraps the TaskEnvelope and verifies payload integrity.
        Raises ValueError if structure is invalid or hash verification fails.
        """
        try:
            import json
            # Unwrap TaskEnvelope from JSON
            envelope_dict = json.loads(payload.decode("utf-8"))
            envelope = TaskEnvelope.from_dict(envelope_dict)
            
            # Verify integrity before deserializing
            if not envelope.verify():
                raise ValueError("TaskEnvelope integrity check failed: hash mismatch")
            
            # Now safely deserialize the cloudpickle payload
            task_data = cloudpickle.loads(envelope.payload)
            if not isinstance(task_data, dict) or "func" not in task_data:
                raise ValueError("Deserialized payload is not a valid MeshWeaver task dictionary")
            return task_data["func"], task_data.get("args", ()), task_data.get("kwargs", {})
        except Exception as e:
            raise ValueError(f"Failed to deserialize task payload: {e}") from e

    @classmethod
    async def execute_task(cls, payload: bytes, task_id: str = "") -> TaskResult:
        """
        Deserialize payload, execute the function (handling async coroutines if needed),
        and return a TaskResult object containing either the cloudpickled result
        or encapsulated error metadata.
        """
        if not task_id:
            task_id = str(uuid.uuid4())

        try:
            func, args, kwargs = cls.deserialize(payload)

            # Check if function is an async coroutine
            if inspect.iscoroutinefunction(func):
                raw_result = await func(*args, **kwargs)
            else:
                raw_result = func(*args, **kwargs)

            # Serialize the return value using cloudpickle
            result_bytes = cloudpickle.dumps(raw_result)

            return TaskResult(
                task_id=task_id,
                success=True,
                result_bytes=result_bytes,
            )

        except Exception as err:
            exc_type, exc_val, exc_tb = sys.exc_info()
            tb_str = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            return TaskResult(
                task_id=task_id,
                success=False,
                error_type=type(err).__name__,
                error_message=str(err),
                traceback=tb_str,
            )

    @staticmethod
    def unpack_result(task_result: TaskResult) -> Any:
        """
        Unpack a TaskResult. Returns the deserialized output if successful,
        or raises RemoteExecutionError if remote execution failed.
        """
        if task_result.success:
            if task_result.result_bytes is None:
                return None
            return cloudpickle.loads(task_result.result_bytes)
        else:
            raise RemoteExecutionError(
                error_type=task_result.error_type or "UnknownError",
                error_message=task_result.error_message or "No error message provided",
                remote_traceback=task_result.traceback or "No remote traceback available",
            )
