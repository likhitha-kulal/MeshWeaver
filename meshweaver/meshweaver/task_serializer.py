"""
<<<<<<< HEAD
MeshWeaver Task Serializer (Execution & Reliability Track - Person B / Likhitha)
=======
MeshWeaver Task Serializer
>>>>>>> 884d6616f2f8d7f38f89eebeaae0f69dec0f2e0d
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
from meshweaver.models import TaskResult


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
        Returns a cloudpickled binary payload.
        """
        if not callable(func):
            raise ValueError(f"Object {func} is not callable")

        task_data = {
            "func": func,
            "args": args,
            "kwargs": kwargs,
        }
        return cloudpickle.dumps(task_data)

    @staticmethod
    def deserialize(payload: bytes) -> Tuple[Callable, Tuple[Any, ...], Dict[str, Any]]:
        """
        Deserialize binary payload into (func, args, kwargs).
        Raises ValueError if structure is invalid.
        """
        try:
            task_data = cloudpickle.loads(payload)
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

<<<<<<< HEAD
=======
            # Check if function is an async coroutine
>>>>>>> 884d6616f2f8d7f38f89eebeaae0f69dec0f2e0d
            if inspect.iscoroutinefunction(func):
                raw_result = await func(*args, **kwargs)
            else:
                raw_result = func(*args, **kwargs)

<<<<<<< HEAD
=======
            # Serialize the return value using cloudpickle
>>>>>>> 884d6616f2f8d7f38f89eebeaae0f69dec0f2e0d
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
