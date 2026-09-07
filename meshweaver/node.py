"""
MeshWeaver Peer Node
Main coordinator combining UDP DHT routing, Gossip health monitoring,
Consensus Leader Election, and TCP task execution.
"""

import argparse
import asyncio
import logging
import os
import sys
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.batch_executor import BatchMetrics, ParallelBatchExecutor
from meshweaver.circuit_breaker import CircuitBreakerConfig, CircuitBreakerRegistry, CircuitState
from meshweaver.gossip import GossipManager
from meshweaver.dht_storage import DHTStorage
from meshweaver.leader_election import (
    ConsensusMetrics,
    ElectionConfig,
    ElectionRole,
    LeaderElectionEngine,
)
from meshweaver.map_reduce import DistributedMapReduce, MapReduceMetrics
from meshweaver.models import Message, NodeID, NodeInfo
from meshweaver.networking import TCPTaskClient, TCPTaskServer, UDPNodeProtocol
from meshweaver.pipeline import PipelineMetrics, TaskPipeline
from meshweaver.routing_table import RoutingTable
from meshweaver.scheduler import RetryPolicy, SchedulingPolicy, TaskScheduler
from meshweaver.task_cache import TaskCache
from meshweaver.task_serializer import RemoteExecutionError, TaskSerializer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("meshweaver.node")


class MeshNode:
    """
    MeshWeaver peer node managing routing, health gossip, consensus leader election,
    and task compute services.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        udp_port: int = 9000,
        tcp_port: Optional[int] = None,
        node_id: Optional[NodeID] = None,
        k: int = 20,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
        election_config: Optional[ElectionConfig] = None,
    ):
        self.host = host
        self.requested_udp_port = udp_port
        self.requested_tcp_port = tcp_port if tcp_port is not None else (udp_port + 1)
        self.node_id = node_id or NodeID()
        self.routing_table = RoutingTable(self.node_id, k=k)

        self.udp_transport: Optional[asyncio.DatagramTransport] = None
        self.udp_protocol: Optional[UDPNodeProtocol] = None
        self.dht_storage: Optional[DHTStorage] = None
        self.tcp_server: Optional[TCPTaskServer] = None
        self.gossip_manager = GossipManager(
            node_id=self.node_id.hex(),
            host=self.host,
            udp_port=udp_port,
            heartbeat_interval=5.0,
            dead_node_timeout=15.0,
        )

        self.bound_udp_port: int = 0
        self.bound_tcp_port: int = 0

        self.circuit_breakers = CircuitBreakerRegistry(default_config=circuit_breaker_config)
        self.scheduler = TaskScheduler(
            local_node_id=self.node_id.hex(),
            gossip_manager=self.gossip_manager,
            circuit_breakers=self.circuit_breakers,
        )
        self.batch_executor = ParallelBatchExecutor(scheduler=self.scheduler)
        self.map_reduce_engine = DistributedMapReduce(scheduler=self.scheduler)
        self.task_cache = TaskCache(dht_storage=None)

        # Leader Election & Consensus Engine
        self.leader_election = LeaderElectionEngine(
            node_id=self.node_id.hex(),
            config=election_config,
            get_active_peers_fn=self._get_active_peers_for_consensus,
            send_message_fn=self._send_consensus_message,
        )

    @property
    def info(self) -> NodeInfo:
        return NodeInfo(
            node_id=self.node_id,
            ip=self.host,
            udp_port=self.bound_udp_port,
            tcp_port=self.bound_tcp_port,
        )

    
    @property
    def is_leader(self) -> bool:
        return self.leader_election.is_leader

    @property
    def leader_id(self) -> Optional[str]:
        return self.leader_election.current_leader

    @property
    def election_role(self) -> ElectionRole:
        return self.leader_election.role

    async def trigger_election(self) -> None:
        """Trigger an immediate leader election."""
        await self.leader_election.start_election()

    def get_consensus_metrics(self) -> ConsensusMetrics:
        """Retrieve real-time leader election metrics snapshot."""
        return self.leader_election.get_consensus_metrics()

    def _get_active_peers_for_consensus(self) -> List[Tuple[str, int]]:
        peers = []
        for p in self.gossip_manager.get_active_peers():
            peers.append((p.ip, p.udp_port))
        return peers

    def _send_consensus_message(self, host: str, port: int, msg: Message) -> None:
        if self.udp_protocol is not None:
            self.udp_protocol.send_datagram(msg, host, port)

    async def start(self) -> None:
        """Start UDP and TCP servers for the node."""
        loop = asyncio.get_running_loop()

        # 1. Start TCP Task Server
        self.tcp_server = TCPTaskServer(
            node_id=self.node_id,
            host=self.host,
            port=self.requested_tcp_port,
        )
        await self.tcp_server.start()
        self.bound_tcp_port = self.tcp_server.port

        # 2. Start UDP Protocol Endpoint
        self.gossip_manager.set_send_callback(
            lambda host, port, payload: self.udp_protocol.send_gossip(host, port, payload)
            if self.udp_protocol is not None else None
        )
        self.gossip_manager.set_receive_callback(self.gossip_manager.receive_heartbeat)

        def _on_vote_response(msg: Message) -> None:
            asyncio.create_task(self.leader_election.handle_vote_response(msg))

        udp_factory = lambda: UDPNodeProtocol(
            node_id=self.node_id,
            tcp_port=self.bound_tcp_port,
            routing_table=self.routing_table,
            gossip_handler=self.gossip_manager.receive_heartbeat,
            consensus_vote_handler=self.leader_election.handle_vote_request,
            consensus_heartbeat_handler=self.leader_election.handle_leader_heartbeat,
            consensus_response_handler=_on_vote_response,
        )
        transport, protocol = await loop.create_datagram_endpoint(
            udp_factory,
            local_addr=(self.host, self.requested_udp_port),
        )
        self.udp_transport = transport
        self.udp_protocol = protocol
        self.bound_udp_port = protocol.local_udp_port

        # Initialize DHT storage
        self.dht_storage = DHTStorage(self.node_id, self.udp_protocol, self.routing_table)
        self.task_cache.dht_storage = self.dht_storage

        # Start Gossip manager and Leader Election
        await self.gossip_manager.start()
        await self.leader_election.start()

        logger.info(
            f"Node {self.node_id.hex()[:8]} running on UDP:{self.bound_udp_port}, "
            f"TCP:{self.bound_tcp_port} (Leader: {self.leader_election.is_leader})"
        )

    async def stop(self) -> None:
        """Gracefully shut down node services."""
        await self.leader_election.stop()
        await self.gossip_manager.stop()
        if self.udp_transport:
            self.udp_transport.close()
        if self.tcp_server:
            await self.tcp_server.stop()
        logger.info(f"Node {self.node_id.hex()[:8]} stopped.")

    async def bootstrap(self, bootstrap_host: str, bootstrap_port: int) -> bool:
        """Join an existing MeshWeaver network via a bootstrap peer."""
        if not self.udp_protocol:
            raise RuntimeError("Node must be started before bootstrapping.")

        logger.info(f"Bootstrapping via {bootstrap_host}:{bootstrap_port}...")
        ping_msg = Message(
            type=MessageType.PING,
            sender_id=self.node_id.hex(),
            sender_udp_port=self.bound_udp_port,
            sender_tcp_port=self.bound_tcp_port,
        )
        try:
            resp = await self.udp_protocol.send_rpc(ping_msg, bootstrap_host, bootstrap_port)
            bootstrap_id = NodeID(resp.sender_id)
            bootstrap_info = NodeInfo(
                node_id=bootstrap_id,
                ip=bootstrap_host,
                udp_port=bootstrap_port,
                tcp_port=resp.sender_tcp_port if resp.sender_tcp_port > 0 else None,
            )
            self.routing_table.add_contact(bootstrap_info)

            # Discover neighbors via iterative lookup
            await self.find_closest_nodes(self.node_id)
            return True
        except Exception as e:
            logger.error(f"Bootstrap failed: {e}")
            return False

    async def find_closest_nodes(self, target: NodeID) -> List[NodeInfo]:
        """Perform iterative Kademlia lookup to locate closest contacts to target."""
        if not self.udp_protocol:
            return []

        shortlist = self.routing_table.find_closest_nodes(target, count=self.routing_table.k)
        visited = {self.node_id}

        for contact in list(shortlist):
            if contact.node_id in visited:
                continue
            visited.add(contact.node_id)

            find_msg = Message(
                type=MessageType.FIND_NODE,
                sender_id=self.node_id.hex(),
                sender_udp_port=self.bound_udp_port,
                sender_tcp_port=self.bound_tcp_port,
                payload={"target_node_id": target.hex()},
            )
            try:
                resp = await self.udp_protocol.send_rpc(
                    find_msg, contact.ip, contact.udp_port, timeout=2.0
                )
                raw_nodes = resp.payload.get("nodes", [])
                for raw in raw_nodes:
                    node_info = NodeInfo.from_dict(raw)
                    self.routing_table.add_contact(node_info)
            except Exception:
                pass

        return self.routing_table.find_closest_nodes(target, count=self.routing_table.k)

    async def submit_task(
        self,
        func: Callable[..., Any],
        *args: Any,
        policy: SchedulingPolicy = SchedulingPolicy.LEAST_LOADED,
        retry_policy: Optional[RetryPolicy] = None,
        use_cache: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Submit a computational task to the mesh with load balancing."""
        if use_cache:
            hit, cached_val = await self.task_cache.get(func, *args, **kwargs)
            if hit:
                logger.info(f"Task cache HIT for {func.__name__}")
                return cached_val

        result = await self.scheduler.schedule_and_execute(
            func=func,
            args=args,
            kwargs=kwargs,
            policy=policy,
            retry_policy=retry_policy,
        )

        if use_cache:
            await self.task_cache.put(func, *args, result=result, **kwargs)

        return result

    async def map(
        self,
        func: Callable[[Any], Any],
        inputs: Iterable[Any],
        chunk_size: int = 1,
        max_concurrency: int = 16,
    ) -> List[Any]:
        """Execute distributed batch map across the peer compute mesh."""
        return await self.batch_executor.map(
            func=func,
            inputs=inputs,
            chunk_size=chunk_size,
            max_concurrency=max_concurrency,
        )

    async def map_reduce(
        self,
        mapper: Callable[[Any], Any],
        reducer: Callable[[Any, Any], Any],
        inputs: Iterable[Any],
        chunk_size: int = 1,
    ) -> Any:
        """Execute a distributed MapReduce computation pipeline."""
        return await self.map_reduce_engine.map_reduce(
            mapper=mapper,
            reducer=reducer,
            inputs=inputs,
            chunk_size=chunk_size,
        )

    def create_pipeline(self, name: str = "Pipeline") -> TaskPipeline:
        """Construct a new DAG pipeline."""
        return TaskPipeline(scheduler=self.scheduler, name=name)


async def run_cli():
    parser = argparse.ArgumentParser(description="MeshWeaver Decentralized Compute Node")
    parser.add_argument("--host", default="127.0.0.1", help="Host IP address to bind")
    parser.add_argument("--port", type=int, default=9000, help="Base UDP port")
    parser.add_argument("--tcp-port", type=int, default=None, help="TCP port for task execution")
    parser.add_argument("--bootstrap-host", default=None, help="Bootstrap peer host IP")
    parser.add_argument("--bootstrap-port", type=int, default=None, help="Bootstrap peer UDP port")

    parser.add_argument("--leader-demo", action="store_true", help="Run consensus leader election demo")
    parser.add_argument("--priority-demo", action="store_true", help="Run priority QoS demonstration")
    args = parser.parse_args()

    node = MeshNode(host=args.host, udp_port=args.port, tcp_port=args.tcp_port)
    await node.start()

    if args.bootstrap_host and args.bootstrap_port:
        await node.bootstrap(args.bootstrap_host, args.bootstrap_port)

    if args.leader_demo:
        print("Starting Leader Election Demo on node...")
        await node.trigger_election()
        await asyncio.sleep(2.0)
        print("Consensus Metrics:", node.get_consensus_metrics().to_dict())

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        await node.stop()


if __name__ == "__main__":
    asyncio.run(run_cli())
