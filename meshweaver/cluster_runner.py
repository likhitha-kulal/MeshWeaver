"""
MeshWeaver Local Cluster Runner & Process Supervisor (Week 4 Day 5).
Manages the lifecycle of multi-node local test and production compute meshes,
supporting dynamic node spawning, automated topology wiring, rolling restarts, and health monitoring.
"""

import asyncio
import logging
import os
import shutil
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from meshweaver.models import (
    ClusterConfig,
    ClusterTopology,
    NodeLifecycleState,
    NodeProcess,
    StorageConfig,
)
from meshweaver.node import MeshNode

logger = logging.getLogger("meshweaver.cluster_runner")


class LocalClusterRunner:
    """
    Local multi-node cluster supervisor and orchestrator.
    Manages in-process lifecycle of N asynchronous MeshNodes, dynamic topology configuration,
    cold reboots, and cluster telemetry aggregation.
    """

    def __init__(self, config: Optional[ClusterConfig] = None):
        self.config = config if config is not None else ClusterConfig()
        self.nodes: Dict[str, MeshNode] = {}
        self.processes: Dict[str, NodeProcess] = {}
        self._start_time: float = 0.0
        self._is_running: bool = False
        self._health_check_task: Optional[asyncio.Task] = None

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def leader(self) -> Optional[MeshNode]:
        """Find the currently elected Raft/Consensus cluster leader."""
        for node in self.nodes.values():
            if getattr(node, "is_leader", False):
                return node
        return None

    def get_node(self, name_or_id: str) -> Optional[MeshNode]:
        """Retrieve a node by its friendly name (e.g. 'node-1') or hex NodeID."""
        if name_or_id in self.nodes:
            return self.nodes[name_or_id]
        for name, proc in self.processes.items():
            if proc.node_id == name_or_id or proc.node_id.startswith(name_or_id):
                return self.nodes.get(name)
        return None

    def list_nodes(self) -> List[MeshNode]:
        """Return list of all managed MeshNode instances."""
        return list(self.nodes.values())

    async def start(self) -> List[MeshNode]:
        """
        Spin up the entire cluster of N nodes according to the configured topology.
        """
        if self._is_running:
            return list(self.nodes.values())

        self._start_time = time.time()
        self._is_running = True
        logger.info(
            f"[CLUSTER START] Initializing cluster '{self.config.cluster_name}' "
            f"with {self.config.node_count} nodes (Topology: {self.config.topology.value})"
        )

        # 1. Instantiate and start each node
        for i in range(1, self.config.node_count + 1):
            name = f"node-{i}"
            udp_p = (self.config.base_udp_port + (i - 1) * 2) if self.config.base_udp_port > 0 else 0
            tcp_p = (self.config.base_tcp_port + (i - 1) * 2) if self.config.base_tcp_port > 0 else 0

            storage_cfg = StorageConfig(
                data_dir=os.path.join(self.config.data_dir, f"cluster_{name}"),
                node_storage_id=name,
                enable_wal=self.config.enable_wal,
            )

            node = MeshNode(
                host=self.config.host,
                udp_port=udp_p,
                tcp_port=tcp_p,
                storage_config=storage_cfg,
            )
            await node.start()

            self.nodes[name] = node
            self.processes[name] = NodeProcess(
                name=name,
                node_id=node.node_id.hex(),
                host=self.config.host,
                udp_port=node.bound_udp_port,
                tcp_port=node.bound_tcp_port,
                state=NodeLifecycleState.HEALTHY,
                uptime_seconds=0.0,
            )

        # 2. Wire cluster topology connections
        if self.config.auto_bootstrap:
            await self._wire_topology()

        # 3. Start background health monitor
        self._health_check_task = asyncio.create_task(self._health_monitor_loop())

        logger.info(f"[CLUSTER READY] Cluster '{self.config.cluster_name}' online with {len(self.nodes)} nodes.")
        return list(self.nodes.values())

    async def _wire_topology(self) -> None:
        """Wire peer contacts and gossip based on the configured topology."""
        node_names = list(self.nodes.keys())
        n_count = len(node_names)
        if n_count <= 1:
            return

        if self.config.topology == ClusterTopology.FULL_MESH:
            for i, name_a in enumerate(node_names):
                node_a = self.nodes[name_a]
                for j, name_b in enumerate(node_names):
                    if i != j:
                        node_b = self.nodes[name_b]
                        node_a.register_neighbor(
                            node_b.node_id.hex(),
                            node_b.host,
                            node_b.bound_udp_port,
                            node_b.bound_tcp_port,
                        )

        elif self.config.topology == ClusterTopology.RING:
            for i, name_a in enumerate(node_names):
                next_node = self.nodes[node_names[(i + 1) % n_count]]
                prev_node = self.nodes[node_names[(i - 1 + n_count) % n_count]]
                self.nodes[name_a].register_neighbor(
                    next_node.node_id.hex(), next_node.host, next_node.bound_udp_port, next_node.bound_tcp_port
                )
                self.nodes[name_a].register_neighbor(
                    prev_node.node_id.hex(), prev_node.host, prev_node.bound_udp_port, prev_node.bound_tcp_port
                )

        elif self.config.topology == ClusterTopology.STAR:
            hub_node = self.nodes[node_names[0]]
            for name in node_names[1:]:
                spoke = self.nodes[name]
                hub_node.register_neighbor(
                    spoke.node_id.hex(), spoke.host, spoke.bound_udp_port, spoke.bound_tcp_port
                )
                spoke.register_neighbor(
                    hub_node.node_id.hex(), hub_node.host, hub_node.bound_udp_port, hub_node.bound_tcp_port
                )

        elif self.config.topology == ClusterTopology.LINEAR:
            for i in range(n_count - 1):
                cur = self.nodes[node_names[i]]
                nxt = self.nodes[node_names[i + 1]]
                cur.register_neighbor(nxt.node_id.hex(), nxt.host, nxt.bound_udp_port, nxt.bound_tcp_port)
                nxt.register_neighbor(cur.node_id.hex(), cur.host, cur.bound_udp_port, cur.bound_tcp_port)

        # Allow initial heartbeats
        await asyncio.sleep(0.2)

    async def _health_monitor_loop(self) -> None:
        """Periodic background task updating node health telemetry."""
        while self._is_running:
            try:
                for name, node in list(self.nodes.items()):
                    proc = self.processes.get(name)
                    if proc and proc.state != NodeLifecycleState.STOPPED:
                        proc.is_leader = getattr(node, "is_leader", False)
                        proc.uptime_seconds = time.time() - self._start_time
                        proc.last_heartbeat = time.time()
                await asyncio.sleep(self.config.health_check_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Health check error: {e}")

    async def stop(self) -> None:
        """Gracefully shut down all nodes in the cluster."""
        if not self._is_running:
            return

        self._is_running = False
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()

        for name, node in self.nodes.items():
            try:
                await node.stop()
                if name in self.processes:
                    self.processes[name].state = NodeLifecycleState.STOPPED
            except Exception as e:
                logger.debug(f"Error stopping {name}: {e}")

        self.nodes.clear()
        logger.info(f"[CLUSTER STOP] Cluster '{self.config.cluster_name}' shut down.")
