"""
Tests for cloudpickle task serialization, SHA-256 TaskEnvelope integrity, and execution.
"""

import asyncio
import json
import unittest

from meshweaver.models import TaskEnvelope, TaskResult
from meshweaver.task_serializer import RemoteExecutionError, TaskSerializer


def add_numbers(a: int, b: int) -> int:
    return a + b


def calculate_factorial(n: int) -> int:
    if n <= 1:
        return 1
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result


async def async_square(x: int) -> int:
    await asyncio.sleep(0.01)
    return x * x


def failing_division(a: float, b: float) -> float:
    return a / b


class TestTaskSerializer(unittest.IsolatedAsyncioTestCase):

    def test_sync_function_serialization(self):
        payload = TaskSerializer.serialize(add_numbers, 10, 25)
        func, args, kwargs = TaskSerializer.deserialize(payload)
        self.assertEqual(func(*args, **kwargs), 35)

    def test_closure_serialization(self):
        multiplier = 7
        closure_fn = lambda x: x * multiplier
        payload = TaskSerializer.serialize(closure_fn, 6)
        func, args, kwargs = TaskSerializer.deserialize(payload)
        self.assertEqual(func(*args, **kwargs), 42)

    async def test_execute_task_sync_and_async(self):
        # Sync execution
        p1 = TaskSerializer.serialize(calculate_factorial, 5)
        r1 = await TaskSerializer.execute_task(p1)
        self.assertTrue(r1.success)
        self.assertEqual(TaskSerializer.unpack_result(r1), 120)

        # Async coroutine execution
        p2 = TaskSerializer.serialize(async_square, 9)
        r2 = await TaskSerializer.execute_task(p2)
        self.assertTrue(r2.success)
        self.assertEqual(TaskSerializer.unpack_result(r2), 81)

    async def test_execute_task_failure_propagation(self):
        payload = TaskSerializer.serialize(failing_division, 10, 0)
        result = await TaskSerializer.execute_task(payload)
        self.assertFalse(result.success)
        self.assertEqual(result.error_type, "ZeroDivisionError")

        with self.assertRaises(RemoteExecutionError) as ctx:
            TaskSerializer.unpack_result(result)
        self.assertEqual(ctx.exception.error_type, "ZeroDivisionError")

    def test_task_envelope_integrity_protection(self):
        payload = b"test payload bytes"
        envelope = TaskEnvelope.wrap(payload)
        self.assertTrue(envelope.verify())

        # Corrupt one byte
        corrupted = bytearray(envelope.payload)
        corrupted[0] ^= 0xFF
        envelope.payload = bytes(corrupted)
        self.assertFalse(envelope.verify())

    async def test_tampered_payload_rejection(self):
        payload = TaskSerializer.serialize(add_numbers, 1, 2)
        envelope_dict = json.loads(payload.decode("utf-8"))
        envelope_dict["sha256"] = "0" * 64
        tampered_payload = json.dumps(envelope_dict).encode("utf-8")

        result = await TaskSerializer.execute_task(tampered_payload)
        self.assertFalse(result.success)
        self.assertEqual(result.error_type, "IntegrityError")


if __name__ == "__main__":
    unittest.main()
