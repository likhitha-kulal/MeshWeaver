"""
MeshWeaver: A zero-dependency, pure Python peer-to-peer async task broker.
"""

from meshweaver.kbucket import KBucket
from meshweaver.models import Message, MessageType, NodeID, NodeInfo, TaskResult
from meshweaver.networking import TCPTaskClient, TCPTaskServer, UDPNodeProtocol
from meshweaver.node import MeshNode
from meshweaver.routing_table import RoutingTable
from meshweaver.task_serializer import RemoteExecutionError, TaskSerializer

__version__ = "0.2.0"

__all__ = [
    "NodeID",
    "NodeInfo",
    "MessageType",
    "Message",
    "TaskResult",
    "KBucket",
    "RoutingTable",
    "UDPNodeProtocol",
    "TCPTaskServer",
    "TCPTaskClient",
    "TaskSerializer",
    "RemoteExecutionError",
    "MeshNode",
]
