"""
MeshWeaver: A zero-dependency, pure Python peer-to-peer async compute mesh.
"""

from meshweaver.gossip import GossipManager, PeerLoadSnapshot
from meshweaver.dht_storage import DHTStorage
from meshweaver.kbucket import KBucket
from meshweaver.models import Message, MessageType, NodeID, NodeInfo, TaskEnvelope, TaskResult
from meshweaver.networking import TCPTaskClient, TCPTaskServer, UDPNodeProtocol
from meshweaver.node import MeshNode
from meshweaver.node_lookup import NodeLookup
from meshweaver.routing_table import RoutingTable
from meshweaver.task_serializer import RemoteExecutionError, TaskSerializer

__version__ = "0.2.0"

__all__ = [
    "NodeID",
    "NodeInfo",
    "MessageType",
    "Message",
    "TaskEnvelope",
    "TaskResult",
    "KBucket",
    "RoutingTable",
    "GossipManager",
    "PeerLoadSnapshot",
    "UDPNodeProtocol",
    "TCPTaskServer",
    "TCPTaskClient",
    "TaskSerializer",
    "RemoteExecutionError",
    "MeshNode",
    "NodeLookup",
    "DHTStorage",
]
