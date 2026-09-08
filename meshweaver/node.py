"""
MeshWeaver Node Controller
Unified coordinator bringing together DHT routing, Gossip health/load monitoring,
intelligent task scheduling, Circuit Breaker resilience, Priority QoS dispatching,
distributed consensus leader election, and Raft replicated state machine engine.
"""

import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from meshweaver.batch_executor import ParallelBatchExecutor
from meshweaver.circuit_breaker import CircuitBreakerRegistry
from meshweaver.dht_storage import DHTStorage
from meshweaver.gossip import GossipProtocol
from meshweaver.leader_election import ElectionConfig, ElectionRole, LeaderElectionEngine
from meshweaver.map_reduce import MapReduceEngine
from meshweaver.models import (
    DistributedLock,
    ElectionRole,
    LockAcquireResult,
    Message,
    NodeID,
    NodeInfo,
    PrioritizedTask,
    RaftCommandType,
    TaskPriority,
    TaskResult,
)
from meshweaver.networking import TCPTaskServer, UDPNodeProtocol
from meshweaver.pipeline import Pipeline
from meshweaver.priority_queue import PriorityDispatcher, PriorityTaskQueue
from meshweaver.raft_log import RaftLog, RaftMetrics, RaftReplicationEngine, ReplicatedStateMachine
from meshweaver.routing_table import RoutingTable
from meshweaver.scheduler import RetryPolicy, TaskScheduler
from meshweaver.task_cache import DHTTaskCache

logger = logging.getLogger("meshweaver.node")


class MeshNode:
    """
    Main node coordinator for MeshWeaver peer-to-peer compute mesh.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        tcp_port: int = 0,
        node_id: Optional[NodeID] = None,
        bootstrap_nodes: Optional[List[Tuple[str, int]]] = None,
        enable_consensus: bool = False,
        election_config: Optional[ElectionConfig] = None,
        enable_raft: bool = True,
    ):
        self.host = host
        self.port = port
        self.tcp_port = tcp_port
        self.node_id = node_id or NodeID()
        self.bootstrap_nodes = bootstrap_nodes or []
        self.enable_consensus = enable_consensus
        self.enable_raft = enable_raft

        self.routing_table = RoutingTable(self.node_id)
        self.udp_transport: Optional[asyncio.DatagramTransport] = None
        self.udp_protocol: Optional[UDPNodeProtocol] = None
        self.tcp_server: Optional[TCPTaskServer] = None

        self.gossip = GossipProtocol(
            node_id=self.node_id,
            routing_table=self.routing_table,
            send_gossip_fn=self._send_gossip_message,
        )

        self.circuit_breaker_registry = CircuitBreakerRegistry()

        self.scheduler = TaskScheduler(
            local_node_id=self.node_id,
            gossip=self.gossip,
            routing_table=self.routing_table,
            circuit_registry=self.circuit_breaker_registry,
        )

        self.dht = DHTStorage(
            local_node_id=self.node_id,
            routing_table=self.routing_table,
            send_store_fn=self._send_store_message,
            send_find_value_fn=self._send_find_value_message,
        )

        self.cache = DHTTaskCache(self.dht)
        self.batch_executor = ParallelBatchExecutor(self)
        self.map_reduce = MapReduceEngine(self)

        self.priority_queue = PriorityTaskQueue()
        self.priority_dispatcher = PriorityDispatcher(self.priority_queue, self)

        # Leader Election Consensus Engine
        self.consensus = LeaderElectionEngine(
            node_id=self.node_id.hex(),
            config=election_config,
            get_active_peers_fn=self._get_active_peer_addrs,
            send_message_fn=self._send_consensus_message,
        )

        # Raft Replicated State Machine Engine
        self.raft_log = RaftLog()
        self.state_machine = ReplicatedStateMachine()
        self.raft = RaftReplicationEngine(
            node_id=self.node_id.hex(),
            log=self.raft_log,
            state_machine=self.state_machine,
            get_active_peers_fn=self._get_active_peer_addrs,
            send_message_fn=self._send_consensus_message,
            get_term_and_role_fn=self._get_term_and_role,
        )

        self._running = False
        self._heartbeat_interval = 2.0

    def _get_active_peer_addrs(self) -> List[Tuple[str, int]]:
        """Return list of (ip, udp_port) for all active contacts in routing table."""
        contacts = self.routing_table.get_all_contacts()
        return [(c.ip, c.udp_port) for c in contacts if c.node_id != self.node_id]

    def _get_term_and_role(self) -> Tuple[int, str]:
        """Return current term and role from leader election or local state."""
        if self.enable_consensus:
            return self.consensus.current_term, self.consensus.role.value
        return 1, "LEADER"

    def _send_gossip_message(self, target_ip: str, target_port: int, payload: Dict[str, object]) -> None:
        if self.udp_protocol:
            self.udp_protocol.send_gossip(target_ip, target_port, payload)

    def _send_consensus_message(self, target_ip: str, target_port: int, msg: Message) -> None:
        if self.udp_protocol:
            self.udp_protocol.send_datagram(msg, target_ip, target_port)

    async def _send_store_message(self, target_ip: str, target_port: int, key: str, value: Any, ttl: float) -> bool:
        if self.udp_protocol:
            return await self.udp_protocol.send_store(target_ip, target_port, key, value, ttl)
        return False

    async def _send_find_value_message(self, target_ip: str, target_port: int, key: str) -> Dict[str, Any]:
        if self.udp_protocol:
            return await self.udp_protocol.send_find_value(target_ip, target_port, key)
        return {"found": False, "nodes": []}

    async def start(self) -> None:
        """Start UDP, TCP, gossip, scheduler, priority dispatcher, consensus, and Raft engines."""
        if self._running:
            return
        self._running = True

        loop = asyncio.get_running_loop()
        self.udp_protocol = UDPNodeProtocol(
            node_id=self.node_id,
            tcp_port=self.tcp_port,
            routing_table=self.routing_table,
            gossip_handler=self.gossip.handle_gossip_message,
            consensus_vote_handler=self.consensus.handle_vote_request,
            consensus_heartbeat_handler=self.consensus.handle_leader_heartbeat,
            consensus_response_handler=self.consensus.handle_vote_response,
            raft_append_entries_handler=self.raft.handle_append_entries_request,
            raft_response_handler=self.raft.handle_append_entries_response,
        )

        transport, _ = await loop.create_datagram_endpoint(
            lambda: self.udp_protocol,
            local_addr=(self.host, self.port),
        )
        self.udp_transport = transport  # type: ignore
        self.port = self.udp_protocol.local_udp_port

        self.tcp_server = TCPTaskServer(self.node_id, self.host, self.tcp_port)
        await self.tcp_server.start()
        self.tcp_port = self.tcp_server.port
        self.udp_protocol.tcp_port = self.tcp_port

        # Add self to routing table
        self.routing_table.add_contact(
            NodeInfo(node_id=self.node_id, ip=self.host, udp_port=self.port, tcp_port=self.tcp_port)
        )

        await self.gossip.start()
        await self.priority_dispatcher.start()

        if self.enable_consensus:
            await self.consensus.start()

        # Bootstrap into existing cluster
        for b_host, b_port in self.bootstrap_nodes:
            await self.bootstrap(b_host, b_port)

        logger.info(f"MeshNode started [{self.node_id.hex()[:8]}] on UDP:{self.port} TCP:{self.tcp_port}")

    async def stop(self) -> None:
        """Gracefully shut down all services and background workers."""
        self._running = False
        await self.priority_dispatcher.stop()
        await self.gossip.stop()

        if self.enable_consensus:
            await self.consensus.stop()

        if self.tcp_server:
            await self.tcp_server.stop()
        if self.udp_transport:
            self.udp_transport.close()
        logger.info(f"MeshNode stopped [{self.node_id.hex()[:8]}]")

    async def bootstrap(self, host: str, port: int) -> bool:
        """Bootstrap node into network by pinging peer and performing FIND_NODE self lookup."""
        if not self.udp_protocol:
            return False
        try:
            await self.udp_protocol.send_ping(host, port, timeout=3.0)
            discovered = await self.udp_protocol.send_find_node(host, port, self.node_id, timeout=3.0)
            for node in discovered:
                self.routing_table.add_contact(node)
            return True
        except Exception as e:
            logger.warning(f"Bootstrap failed for {host}:{port}: {e}")
            return False

    async def submit_task(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Schedule and execute a task across the mesh cluster."""
        return await self.scheduler.schedule_and_execute(func, *args, **kwargs)

    async def submit_priority_task(
        self,
        func: Callable,
        *args: Any,
        priority: TaskPriority = TaskPriority.NORMAL,
        deadline_seconds: Optional[float] = None,
        **kwargs: Any,
    ) -> PrioritizedTask:
        """Submit a prioritized task to the local QoS priority queue."""
        return await self.priority_dispatcher.submit_task(
            func, *args, priority=priority, deadline_seconds=deadline_seconds, **kwargs
        )

    # Replicated State Machine Public APIs
    async def state_set(self, key: str, value: Any, timeout: float = 5.0) -> Any:
        """Atomically set a key/value pair across the replicated state machine."""
        return await self.raft.propose_command(
            command_type=RaftCommandType.SET,
            key=key,
            value=value,
            timeout=timeout,
        )

    def state_get(self, key: str, default: Any = None) -> Any:
        """Retrieve value for key from local replicated state machine view."""
        return self.state_machine.get(key, default)

    async def state_delete(self, key: str, timeout: float = 5.0) -> Any:
        """Atomically remove a key from the replicated state machine."""
        return await self.raft.propose_command(
            command_type=RaftCommandType.DELETE,
            key=key,
            timeout=timeout,
        )

    async def state_cas(self, key: str, expected: Any, new_value: Any, timeout: float = 5.0) -> bool:
        """Atomic Compare-And-Swap (CAS) on replicated state machine."""
        return await self.raft.propose_command(
            command_type=RaftCommandType.CAS,
            key=key,
            value=new_value,
            extra_data={"expected": expected},
            timeout=timeout,
        )

    async def state_increment(self, key: str, delta: int = 1, timeout: float = 5.0) -> int:
        """Atomic monotonic counter increment on replicated state machine."""
        return await self.raft.propose_command(
            command_type=RaftCommandType.INCREMENT,
            key=key,
            extra_data={"delta": delta},
            timeout=timeout,
        )

    async def acquire_lock(self, resource: str, ttl_seconds: float = 30.0, timeout: float = 5.0) -> LockAcquireResult:
        """Acquire a distributed mutual exclusion lease with monotonic fencing tokens."""
        return await self.raft.propose_command(
            command_type=RaftCommandType.LOCK_ACQUIRE,
            key=resource,
            extra_data={"ttl_seconds": ttl_seconds},
            timeout=timeout,
        )

    async def release_lock(self, resource: str, fencing_token: Optional[int] = None, timeout: float = 5.0) -> bool:
        """Release an acquired distributed lock."""
        return await self.raft.propose_command(
            command_type=RaftCommandType.LOCK_RELEASE,
            key=resource,
            fencing_token=fencing_token,
            timeout=timeout,
        )

    def get_raft_metrics(self) -> RaftMetrics:
        """Capture live Raft log replication and state machine metrics."""
        return self.raft.get_raft_metrics()

    def create_pipeline(self, name: str = "Pipeline") -> Pipeline:
        """Factory method to construct a composable multi-stage computation DAG pipeline."""
        return Pipeline(mesh=self, name=name)
