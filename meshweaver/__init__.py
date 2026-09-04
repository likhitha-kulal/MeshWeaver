"""
MeshWeaver: A zero-dependency, pure Python peer-to-peer async compute mesh.
"""

from meshweaver.batch_executor import BatchMetrics, ParallelBatchExecutor, chunk_iterable
from meshweaver.circuit_breaker import (
    BreakerMetrics,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitBreakerRegistry,
    CircuitState,
)
from meshweaver.gossip import GossipManager, PeerLoadSnapshot
from meshweaver.dht_storage import DHTStorage
from meshweaver.kbucket import KBucket
from meshweaver.map_reduce import DistributedMapReduce, MapReduceMetrics
from meshweaver.models import Message, MessageType, NodeID, NodeInfo, TaskEnvelope, TaskResult
from meshweaver.networking import TCPTaskClient, TCPTaskServer, UDPNodeProtocol
from meshweaver.node import MeshNode
from meshweaver.node_lookup import NodeLookup
from meshweaver.priority_queue import (
    PrioritizedTask,
    PriorityDispatcher,
    PriorityMetrics,
    PriorityTaskQueue,
    TaskPriority,
)
from meshweaver.pipeline import PipelineMetrics, PipelineStage, StageMetrics, TaskPipeline
from meshweaver.routing_table import RoutingTable
from meshweaver.scheduler import (
    LoadScorer,
    RetryPolicy,
    SchedulingPolicy,
    TaskScheduler,
    WorkerCandidate,
)
from meshweaver.task_cache import TaskCache
from meshweaver.task_serializer import RemoteExecutionError, TaskSerializer

__version__ = "0.3.6"

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
    "TaskScheduler",
    "SchedulingPolicy",
    "RetryPolicy",
    "LoadScorer",
    "WorkerCandidate",
    "TaskCache",
    "ParallelBatchExecutor",
    "BatchMetrics",
    "chunk_iterable",
    "DistributedMapReduce",
    "MapReduceMetrics",
    "TaskPipeline",
    "PipelineStage",
    "PipelineMetrics",
    "StageMetrics",
    "CircuitBreaker",
    "CircuitState",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
    "CircuitBreakerRegistry",
    "BreakerMetrics",
    "TaskPriority",
    "PrioritizedTask",
    "PriorityTaskQueue",
    "PriorityDispatcher",
    "PriorityMetrics",
]

