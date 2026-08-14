"""
MeshNode Entrypoint (Execution & Reliability Track - Person B / Likhitha)
Main class and CLI entrypoint for launching TaskServer and submitting cloudpickled tasks.
"""

import argparse
import asyncio
import logging
import os
import sys
from typing import Any, Callable, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.networking import TCPTaskClient, TCPTaskServer
from meshweaver.task_serializer import RemoteExecutionError, TaskSerializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("meshweaver.node")


class TaskExecutionNode:
    """
    MeshWeaver node managing TCP TaskServer for task execution and submission.
    """

    def __init__(self, host: str = "127.0.0.1", tcp_port: int = 9001):
        self.host = host
        self.requested_tcp_port = tcp_port
        self.tcp_server: Optional[TCPTaskServer] = None
        self.bound_tcp_port: int = 0

    async def start(self) -> None:
        """Start TCP Task Server."""
        self.tcp_server = TCPTaskServer(host=self.host, port=self.requested_tcp_port)
        await self.tcp_server.start()
        self.bound_tcp_port = self.tcp_server.port

        logger.info(
            f"=== MeshWeaver Task Node Started ===\n"
            f"  Host     : {self.host}\n"
            f"  TCP Port : {self.bound_tcp_port}\n"
            f"====================================="
        )

    async def stop(self) -> None:
        """Stop TCP Task Server."""
        if self.tcp_server:
            await self.tcp_server.stop()
        logger.info("TaskNode stopped.")

    async def submit_task(self, target_host: str, target_tcp_port: int, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Serialize a task and submit it to a remote node's TaskServer."""
        payload_bytes = TaskSerializer.serialize(func, *args, **kwargs)
        task_result = await TCPTaskClient.send_task(target_host, target_tcp_port, payload_bytes)
        return TaskSerializer.unpack_result(task_result)


# --- Demo Functions for CLI testing ---
def sample_add(a: int, b: int) -> int:
    return a + b


def sample_fibonacci(n: int) -> int:
    if n <= 0:
        return 0
    elif n == 1:
        return 1
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b


def sample_failing_task() -> float:
    return 1 / 0


async def cli_main() -> None:
    parser = argparse.ArgumentParser(description="MeshWeaver Task Execution Node (Person B)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Binding host address")
    parser.add_argument("--port", type=int, default=9001, help="TCP port for task server")
    parser.add_argument("--task-target-port", type=int, default=None, help="Target TCP port to send demo tasks to")
    parser.add_argument("--demo-task", action="store_true", help="Submit demo tasks to target node")

    args = parser.parse_args()

    node = TaskExecutionNode(host=args.host, tcp_port=args.port)
    await node.start()

    try:
        if args.demo_task and args.task_target_port:
            logger.info(f"--- Submitting Demo Tasks to {args.host}:{args.task_target_port} ---")
            
            # Fibonacci
            fib_res = await node.submit_task(args.host, args.task_target_port, sample_fibonacci, 20)
            logger.info(f"Remote Task Result: sample_fibonacci(20) = {fib_res}")

            # Addition
            add_res = await node.submit_task(args.host, args.task_target_port, sample_add, 42, 58)
            logger.info(f"Remote Task Result: sample_add(42, 58) = {add_res}")

            # Exception test
            try:
                await node.submit_task(args.host, args.task_target_port, sample_failing_task)
            except RemoteExecutionError as err:
                logger.info(f"Caught RemoteExecutionError:\n  Error Type: {err.error_type}\n  Message   : {err.error_message}")
        else:
            logger.info("Node running in TaskServer mode. Press Ctrl+C to stop.")
            await asyncio.Event().wait()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(cli_main())
