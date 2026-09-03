"""
MeshWeaver Peer Node
Main coordinator combining UDP DHT routing, Gossip health monitoring, and TCP task execution.
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
    MeshWeaver peer node managing routing, health gossip, and task compute services.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        udp_port: int = 9000,
        tcp_port: Optional[int] = None,
        node_id: Optional[NodeID] = None,
        k: int = 20,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
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

    @property
    def info(self) -> NodeInfo:
        return NodeInfo(
            node_id=self.node_id,
            ip=self.host,
            udp_port=self.bound_udp_port,
            tcp_port=self.bound_tcp_port,
        )

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

        udp_factory = lambda: UDPNodeProtocol(
            node_id=self.node_id,
            tcp_port=self.bound_tcp_port,
            routing_table=self.routing_table,
            gossip_handler=self.gossip_manager.receive_heartbeat,
        )
        transport, protocol = await loop.create_datagram_endpoint(
            udp_factory,
            local_addr=(self.host, self.requested_udp_port),
        )
        self.udp_transport = transport  # type: ignore
        self.udp_protocol = protocol  # type: ignore
        self.dht_storage = DHTStorage(self.udp_protocol)
        self.task_cache.dht_storage = self.dht_storage
        self.bound_udp_port = self.udp_protocol.local_udp_port

        self.gossip_manager.bind_network(self.udp_protocol)
        self.gossip_manager.set_send_callback(
            lambda host, port, payload: self.udp_protocol.send_gossip(host, port, payload)
            if self.udp_protocol is not None else None
        )
        await self.gossip_manager.start()

        logger.info(
            f"=== MeshWeaver Node Online ===\n"
            f"  Node ID  : {self.node_id.hex()}\n"
            f"  Host     : {self.host}\n"
            f"  UDP Port : {self.bound_udp_port}\n"
            f"  TCP Port : {self.bound_tcp_port}\n"
            f"=============================="
        )

    async def stop(self) -> None:
        """Gracefully shut down all node networking and background services."""
        if self.gossip_manager:
            await self.gossip_manager.stop()

        if self.tcp_server:
            await self.tcp_server.stop()

        if self.udp_transport and not self.udp_transport.is_closing():
            self.udp_transport.close()

        logger.info("MeshNode stopped.")

    def register_neighbor(self, node_id: str, host: str, udp_port: int, tcp_port: Optional[int] = None) -> None:
        """Register a known neighbor for gossip heartbeat exchange."""
        self.gossip_manager.register_neighbor(node_id, host, udp_port, tcp_port)

    async def ping(self, target_host: str, target_udp_port: int, timeout: float = 5.0) -> Message:
        """Ping a remote node to check liveness."""
        if not self.udp_protocol:
            raise RuntimeError("Node is not running")
        return await self.udp_protocol.send_ping(target_host, target_udp_port, timeout=timeout)

    async def find_node(
        self,
        target_host: str,
        target_udp_port: int,
        target_id: NodeID,
        timeout: float = 5.0,
    ) -> List[NodeInfo]:
        """Query a remote peer for the closest nodes to a target NodeID."""
        if not self.udp_protocol:
            raise RuntimeError("Node is not running")
        return await self.udp_protocol.send_find_node(
            target_ip=target_host,
            target_port=target_udp_port,
            target_id=target_id,
            timeout=timeout,
        )

    async def store(self, key: str, value: Any, ttl: float, timeout: float = 5.0) -> int:
        """Store a value through the running node's DHT."""
        if self.dht_storage is None:
            raise RuntimeError("Node is not running")
        return await self.dht_storage.store(key, value, ttl, timeout=timeout)

    async def find_value(self, key: str, timeout: float = 5.0) -> Any:
        """Find a value through the running node's DHT, or return None."""
        if self.dht_storage is None:
            raise RuntimeError("Node is not running")
        return await self.dht_storage.find_value(key, timeout=timeout)

    async def bootstrap(
        self,
        bootstrap_nodes: List[Tuple[str, int]],
        timeout: float = 5.0,
    ) -> int:
        """
        Join network by discovering peers through bootstrap nodes.
        Returns the count of newly discovered contacts added to routing table.
        """
        if not self.udp_protocol:
            raise RuntimeError("Node is not running")

        initial_count = self.routing_table.total_contacts()
        for ip, port in bootstrap_nodes:
            try:
                pong = await self.ping(ip, port, timeout=timeout)
                discovered = await self.find_node(ip, port, self.node_id, timeout=timeout)
                for contact in discovered:
                    if contact.node_id != self.node_id:
                        try:
                            await self.ping(contact.ip, contact.udp_port, timeout=2.0)
                        except Exception:
                            pass
            except Exception as e:
                logger.warning(f"Bootstrap peer {ip}:{port} unreachable: {e}")

        final_count = self.routing_table.total_contacts()
        return final_count - initial_count

    async def submit_task(
        self,
        target_host: str,
        target_tcp_port: int,
        func: Callable,
        *args: Any,
        timeout: float = 30.0,
        **kwargs: Any,
    ) -> Any:
        """Submit a serialized callable directly to a specific target node over TCP."""
        payload_bytes = TaskSerializer.serialize(func, *args, **kwargs)
        task_result = await TCPTaskClient.send_task(
            target_host,
            target_tcp_port,
            payload_bytes,
            timeout=timeout,
        )
        return TaskSerializer.unpack_result(task_result)

    async def schedule_task(
        self,
        func: Callable,
        *args: Any,
        policy: Optional[SchedulingPolicy] = None,
        retry_policy: Optional[RetryPolicy] = None,
        fallback_local: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Schedule and dispatch task with intelligent load balancing and failover."""
        return await self.scheduler.dispatch_task(
            func,
            *args,
            policy=policy,
            retry_policy=retry_policy,
            fallback_local=fallback_local,
            **kwargs,
        )

    async def map(
        self,
        func: Callable[[Any], Any],
        iterable: Any,
        chunk_size: int = 1,
        concurrency: Optional[int] = None,
        policy: Optional[SchedulingPolicy] = None,
        retry_policy: Optional[RetryPolicy] = None,
        return_exceptions: bool = False,
    ) -> Tuple[List[Any], BatchMetrics]:
        """Distribute batch workload across the mesh concurrently."""
        return await self.batch_executor.map(
            func=func,
            iterable=iterable,
            chunk_size=chunk_size,
            concurrency=concurrency,
            policy=policy,
            retry_policy=retry_policy,
            return_exceptions=return_exceptions,
        )

    async def map_reduce(
        self,
        map_fn: Callable[[Any], List[Tuple[Any, Any]]],
        reduce_fn: Callable[[Any, List[Any]], Any],
        data: Iterable[Any],
        chunk_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        policy: SchedulingPolicy = SchedulingPolicy.LEAST_LOADED,
    ) -> Tuple[Dict[Any, Any], MapReduceMetrics]:
        """Execute distributed MapReduce across mesh cluster."""
        return await self.map_reduce_engine.execute_map_reduce(
            map_fn=map_fn,
            reduce_fn=reduce_fn,
            data=data,
            chunk_size=chunk_size,
            concurrency=concurrency,
            policy=policy,
        )

    async def tree_reduce(
        self,
        reduce_fn: Callable[[Any, Any], Any],
        data: Iterable[Any],
        initial_value: Optional[Any] = None,
        branching_factor: int = 2,
        policy: SchedulingPolicy = SchedulingPolicy.LEAST_LOADED,
    ) -> Any:
        """Execute parallel hierarchical tree reduction across mesh workers."""
        return await self.map_reduce_engine.tree_reduce(
            reduce_fn=reduce_fn,
            data=data,
            initial_value=initial_value,
            branching_factor=branching_factor,
            policy=policy,
        )

    def create_pipeline(self) -> TaskPipeline:
        """Create a new composable task pipeline tied to this node's scheduler."""
        return TaskPipeline(scheduler=self.scheduler)

    async def cached_compute(
        self,
        func: Callable,
        *args: Any,
        ttl: Optional[float] = None,
        force_refresh: bool = False,
        policy: Optional[SchedulingPolicy] = None,
        **kwargs: Any,
    ) -> Any:
        """Execute computation with automatic DHT memoization."""
        async def _dispatch_wrapper(f: Callable, *a: Any, **kw: Any) -> Any:
            return await self.schedule_task(f, *a, policy=policy, **kw)

        return await self.task_cache.execute_with_cache(
            func,
            *args,
            executor_func=_dispatch_wrapper,
            ttl=ttl,
            force_refresh=force_refresh,
            **kwargs,
        )

    def get_circuit_status(self, node_id: Optional[str] = None) -> Dict[str, Any]:
        """Return circuit breaker status for a specific node or all registered nodes."""
        if node_id:
            cb = self.circuit_breakers.get_or_create(node_id)
            return {
                "node_id": node_id,
                "state": cb.state.value,
                "failure_count": cb.failure_count,
                "success_count": cb.success_count,
                "is_available": cb.is_available(),
            }
        return {
            nid: {
                "state": cb.state.value,
                "failure_count": cb.failure_count,
                "success_count": cb.success_count,
                "is_available": cb.is_available(),
            }
            for nid, cb in self.circuit_breakers._breakers.items()
        }

    def get_tripped_nodes(self) -> List[str]:
        """Return list of node IDs whose circuit breakers are currently OPEN."""
        return self.circuit_breakers.get_tripped_nodes()

    def reset_circuit_breakers(self) -> None:
        """Reset all circuit breakers to CLOSED state."""
        self.circuit_breakers.clear()


# --- Builtin helper tasks for CLI demonstration ---
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


def sample_word_mapper(line: str) -> List[Tuple[str, int]]:
    words = line.lower().split()
    return [(w.strip(".,!?:;"), 1) for w in words if w.strip(".,!?:;")]


def sample_word_reducer(word: str, counts: List[int]) -> int:
    return sum(counts)


async def cli_main() -> None:
    parser = argparse.ArgumentParser(description="MeshWeaver P2P Compute Mesh Node")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Binding IP host")
    parser.add_argument("--port", type=int, default=9000, help="Base UDP port (TCP port defaults to port + 1)")
    parser.add_argument("--tcp-port", type=int, default=None, help="Explicit TCP task server port")
    parser.add_argument("--ping-host", type=str, default=None, help="Peer host to PING")
    parser.add_argument("--ping-port", type=int, default=None, help="Peer UDP port to PING")
    parser.add_argument("--bootstrap-host", type=str, default=None, help="Bootstrap node IP")
    parser.add_argument("--bootstrap-port", type=int, default=None, help="Bootstrap node UDP port")
    parser.add_argument("--find-node", type=str, default=None, help="Target NodeID hex to query")
    parser.add_argument("--task-target-port", type=int, default=None, help="Target TCP port for task submission")
    parser.add_argument("--demo-task", action="store_true", help="Submit demo tasks")
    parser.add_argument(
        "--scheduler-policy",
        type=str,
        default="least_loaded",
        choices=["least_loaded", "round_robin", "power_of_two_random", "local_first"],
        help="Task scheduling policy to use for automated dispatch",
    )
    parser.add_argument("--batch-demo", action="store_true", help="Run parallel distributed batch demo")
    parser.add_argument("--cache-demo", action="store_true", help="Run DHT cached compute demo")
    parser.add_argument("--mapreduce-demo", action="store_true", help="Run distributed MapReduce word count demo")
    parser.add_argument("--pipeline-demo", action="store_true", help="Run multi-stage pipeline compute demo")
    parser.add_argument("--circuit-demo", action="store_true", help="Run circuit breaker fault-tolerance demo")
    parser.add_argument("--circuit-reset", action="store_true", help="Reset all circuit breakers to CLOSED")

    args = parser.parse_args()

    node = MeshNode(
        host=args.host,
        udp_port=args.port,
        tcp_port=args.tcp_port,
    )

    await node.start()

    try:
        if args.circuit_reset:
            node.reset_circuit_breakers()
            logger.info("All node circuit breakers have been reset to CLOSED state.")

        if args.ping_host and args.ping_port:
            pong = await node.ping(args.ping_host, args.ping_port)
            logger.info(f"PONG received from {pong.sender_id[:8]}... (status={pong.payload.get('status')})")

        if args.bootstrap_host and args.bootstrap_port:
            discovered = await node.bootstrap([(args.bootstrap_host, args.bootstrap_port)])
            logger.info(f"Bootstrap completed. Discovered {discovered} new peers.")

        if args.find_node and args.ping_host and args.ping_port:
            target_id = NodeID(args.find_node)
            nodes = await node.find_node(args.ping_host, args.ping_port, target_id)
            logger.info(f"Discovered {len(nodes)} nearest nodes:")
            for n in nodes:
                logger.info(f"  -> {n.node_id.hex()} @ {n.ip}:{n.udp_port}")

        if args.demo_task and args.task_target_port:
            fib_res = await node.submit_task(args.host, args.task_target_port, sample_fibonacci, 20)
            logger.info(f"Remote Task Result: fibonacci(20) = {fib_res}")
            add_res = await node.submit_task(args.host, args.task_target_port, sample_add, 42, 58)
            logger.info(f"Remote Task Result: add(42, 58) = {add_res}")

        if args.batch_demo:
            policy = SchedulingPolicy(args.scheduler_policy)
            logger.info(f"Executing batch demo with policy={policy.value}...")
            batch_inputs = list(range(1, 11))
            results, metrics = await node.map(sample_fibonacci, batch_inputs, policy=policy)
            logger.info(f"Batch completed: {metrics.completed_items}/{metrics.total_items} in {metrics.duration_seconds}s (Throughput: {metrics.throughput} items/s)")
            logger.info(f"Batch results: {results}")

        if args.cache_demo:
            logger.info("Executing DHT cached compute demo...")
            res1 = await node.cached_compute(sample_fibonacci, 35, ttl=120)
            logger.info(f"First compute (computed): fib(35) = {res1}")
            res2 = await node.cached_compute(sample_fibonacci, 35, ttl=120)
            logger.info(f"Second compute (cache hit): fib(35) = {res2}")

        if args.mapreduce_demo:
            sample_docs = [
                "MeshWeaver is a peer to peer distributed compute mesh",
                "Distributed computing enables scalable parallel batch workflows",
                "Kademlia DHT provides decentralized routing and peer lookup",
                "Task scheduler balances CPU and RAM load across worker nodes",
                "MeshWeaver handles automatic failover and task retry seamlessly",
            ]
            logger.info(f"Executing distributed MapReduce demo across {len(sample_docs)} lines...")
            counts, mr_metrics = await node.map_reduce(
                sample_word_mapper,
                sample_word_reducer,
                sample_docs,
            )
            logger.info(
                f"MapReduce completed in {mr_metrics.total_duration_seconds:.3f}s (Map: {mr_metrics.map_duration_seconds:.3f}s, Shuffle: {mr_metrics.shuffle_duration_seconds:.3f}s, Reduce: {mr_metrics.reduce_duration_seconds:.3f}s)"
            )
            top_words = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:5]
            logger.info(f"Top 5 words: {top_words}")

        if args.pipeline_demo:
            logger.info("Executing multi-stage pipeline demo...")
            pipeline = node.create_pipeline()
            pipeline.pipe("Stage 1: Square", lambda x: x * x)
            pipeline.pipe("Stage 2: Add Constant", lambda x: x + 10)
            pipeline.add_stage("Stage 3: Aggregate Sum", lambda lst: sum(lst), is_parallel=False)

            inputs = [1, 2, 3, 4, 5]
            out, p_metrics = await pipeline.execute(inputs)
            logger.info(f"Pipeline executed {len(p_metrics.stages)} stages in {p_metrics.total_duration_seconds:.3f}s. Result = {out}")

        if args.circuit_demo:
            logger.info("Executing Circuit Breaker resilience status demo...")
            stats = node.scheduler.get_stats()
            logger.info(f"Scheduler Load & Circuit Breaker Stats: {stats}")
            circuits = node.get_circuit_status()
            logger.info(f"Active Circuit Breakers ({len(circuits)} registered):")
            for nid, status in circuits.items():
                logger.info(f"  -> Node {nid[:8]}... State={status['state']}, Failures={status['failure_count']}, Available={status['is_available']}")

        if not (args.ping_host or args.bootstrap_host or args.demo_task or args.batch_demo or args.cache_demo or args.mapreduce_demo or args.pipeline_demo or args.circuit_demo or args.circuit_reset):
            logger.info("Node running. Press Ctrl+C to shutdown.")
            await asyncio.Event().wait()

    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(cli_main())
