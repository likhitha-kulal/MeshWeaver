"""
Tests for UDP discovery, FIND_NODE RPCs, and TCP task execution transport.
"""

import asyncio
import unittest

from meshweaver.models import MessageType, NodeID, NodeInfo
from meshweaver.node import MeshNode
from meshweaver.task_serializer import RemoteExecutionError


def multiply(a: int, b: int) -> int:
    return a * b


def faulty_task() -> None:
    raise ValueError("Explicit test error")


class TestNetworking(unittest.IsolatedAsyncioTestCase):

    async def test_udp_ping_pong_exchange(self):
        node_a = MeshNode(host="127.0.0.1", udp_port=18000, tcp_port=18001)
        node_b = MeshNode(host="127.0.0.1", udp_port=18002, tcp_port=18003)

        await node_a.start()
        await node_b.start()

        try:
            pong = await node_a.ping("127.0.0.1", node_b.bound_udp_port, timeout=3.0)
            self.assertEqual(pong.type, MessageType.PONG)
            self.assertEqual(pong.sender_id, node_b.node_id.hex())
            self.assertEqual(pong.payload.get("status"), "OK")
        finally:
            await node_a.stop()
            await node_b.stop()

    async def test_find_node_rpc(self):
        node_a = MeshNode(host="127.0.0.1", udp_port=18010, tcp_port=18011)
        node_b = MeshNode(host="127.0.0.1", udp_port=18012, tcp_port=18013)

        # Seed node B with peer contacts
        p1 = NodeInfo(NodeID(), "127.0.0.1", 18014)
        p2 = NodeInfo(NodeID(), "127.0.0.1", 18016)
        node_b.routing_table.add_contact(p1)
        node_b.routing_table.add_contact(p2)

        await node_a.start()
        await node_b.start()

        try:
            target_id = NodeID()
            discovered = await node_a.find_node(
                "127.0.0.1",
                node_b.bound_udp_port,
                target_id,
                timeout=3.0,
            )
            self.assertGreaterEqual(len(discovered), 2)
            discovered_ids = {n.node_id.hex() for n in discovered}
            self.assertIn(p1.node_id.hex(), discovered_ids)
            self.assertIn(p2.node_id.hex(), discovered_ids)
        finally:
            await node_a.stop()
            await node_b.stop()

    async def test_remote_tcp_task_execution(self):
        server_node = MeshNode(host="127.0.0.1", udp_port=18020, tcp_port=18021)
        client_node = MeshNode(host="127.0.0.1", udp_port=18022, tcp_port=18023)

        await server_node.start()
        await client_node.start()

        try:
            result = await client_node.submit_task(
                "127.0.0.1",
                server_node.bound_tcp_port,
                multiply,
                12,
                12,
            )
            self.assertEqual(result, 144)

            with self.assertRaises(RemoteExecutionError) as ctx:
                await client_node.submit_task(
                    "127.0.0.1",
                    server_node.bound_tcp_port,
                    faulty_task,
                )
            self.assertEqual(ctx.exception.error_type, "ValueError")
        finally:
            await server_node.stop()
            await client_node.stop()


if __name__ == "__main__":
    unittest.main()
