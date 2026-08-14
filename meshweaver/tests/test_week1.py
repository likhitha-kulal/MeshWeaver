"""
MeshWeaver Week 1 Test Suite (Execution & Reliability Track - Person B / Likhitha)
Tests cloudpickle task serialization, local execution, error handling, and TCP task server/client.
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from meshweaver.node import TaskExecutionNode
from meshweaver.task_serializer import RemoteExecutionError, TaskSerializer


def add(a: int, b: int) -> int:
    return a + b


def fibonacci(n: int) -> int:
    if n <= 0:
        return 0
    elif n == 1:
        return 1
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b


def divide(a: float, b: float) -> float:
    return a / b


async def async_multiplier(a: int, b: int) -> int:
    await asyncio.sleep(0.01)
    return a * b


class TestWeek1Execution(unittest.IsolatedAsyncioTestCase):

    def test_task_serialization_sync_and_async(self):
        """Test serializing and local execution of sync, closure, and async functions."""
        # 1. Sync addition
        payload_add = TaskSerializer.serialize(add, 15, 25)
        func, args, kwargs = TaskSerializer.deserialize(payload_add)
        self.assertEqual(func(*args, **kwargs), 40)

        # 2. Closure / Lambda function
        factor = 10
        multiply_closure = lambda x: x * factor
        payload_closure = TaskSerializer.serialize(multiply_closure, 5)
        f_closure, c_args, c_kwargs = TaskSerializer.deserialize(payload_closure)
        self.assertEqual(f_closure(*c_args, **c_kwargs), 50)

    async def test_task_serializer_execute_task_success(self):
        """Test TaskSerializer.execute_task for sync and coroutine functions."""
        payload = TaskSerializer.serialize(async_multiplier, 6, 7)
        task_result = await TaskSerializer.execute_task(payload)

        self.assertTrue(task_result.success)
        result_val = TaskSerializer.unpack_result(task_result)
        self.assertEqual(result_val, 42)

    async def test_task_serializer_execute_task_failure(self):
        """Test TaskSerializer.execute_task when function raises an exception."""
        payload = TaskSerializer.serialize(divide, 10, 0)
        task_result = await TaskSerializer.execute_task(payload)

        self.assertFalse(task_result.success)
        self.assertEqual(task_result.error_type, "ZeroDivisionError")
        self.assertIn("division by zero", task_result.error_message)

        with self.assertRaises(RemoteExecutionError) as ctx:
            TaskSerializer.unpack_result(task_result)

        self.assertEqual(ctx.exception.error_type, "ZeroDivisionError")

    async def test_remote_task_execution(self):
        """Test remote task execution over TCP between Node A and Node B."""
        node_a = TaskExecutionNode(host="127.0.0.1", tcp_port=19101)
        node_b = TaskExecutionNode(host="127.0.0.1", tcp_port=19103)

        await node_a.start()
        await node_b.start()

        try:
            # 1. Execute Fibonacci on Node B from Node A
            fib_result = await node_a.submit_task(
                "127.0.0.1", node_b.bound_tcp_port, fibonacci, 10
            )
            self.assertEqual(fib_result, 55)

            # 2. Execute Addition on Node B from Node A
            add_result = await node_a.submit_task(
                "127.0.0.1", node_b.bound_tcp_port, add, 123, 456
            )
            self.assertEqual(add_result, 579)

            # 3. Submit failing task (division by zero)
            with self.assertRaises(RemoteExecutionError) as ctx:
                await node_a.submit_task(
                    "127.0.0.1", node_b.bound_tcp_port, divide, 5, 0
                )

            self.assertEqual(ctx.exception.error_type, "ZeroDivisionError")

        finally:
            await node_a.stop()
            await node_b.stop()


if __name__ == "__main__":
    unittest.main()
