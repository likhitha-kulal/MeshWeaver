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

    async def kill_node(self, name_or_id: str, simulated_crash: bool = True) -> bool:
        """
        Simulate immediate node failure or crash.
        Stops the target node process and marks its lifecycle state as CRASHED.
        """
        node = self.get_node(name_or_id)
        if not node:
            return False

        name = None
        for n, inst in self.nodes.items():
            if inst == node:
                name = n
                break
        if not name:
            return False

        try:
            await node.stop()
        except Exception as e:
            logger.debug(f"Error during node kill {name}: {e}")

        if name in self.processes:
            self.processes[name].state = NodeLifecycleState.CRASHED if simulated_crash else NodeLifecycleState.STOPPED

        # Remove from active nodes dict so cluster routing bypasses it
        self.nodes.pop(name, None)
        logger.warning(f"[CLUSTER NODE CRASH] Node '{name}' ({node.node_id.hex()[:8]}) was killed/crashed.")
        return True

    async def restart_node(self, name_or_id: str) -> Optional[MeshNode]:
        """
        Cold-reboot a crashed or stopped node.
        Re-instantiates the MeshNode using its original StorageConfig so WAL replay restores state.
        Re-connects the node with active cluster peers.
        """
        name = None
        for n, proc in self.processes.items():
            if n == name_or_id or proc.node_id == name_or_id or proc.node_id.startswith(name_or_id):
                name = n
                break
        if not name or name not in self.processes:
            return None

        proc = self.processes[name]
        logger.info(f"[CLUSTER RESTART] Rebooting node '{name}' with cold-boot WAL replay...")

        storage_cfg = StorageConfig(
            data_dir=os.path.join(self.config.data_dir, f"cluster_{name}"),
            node_storage_id=name,
            enable_wal=self.config.enable_wal,
        )

        rebooted_node = MeshNode(
            host=proc.host,
            udp_port=proc.udp_port,
            tcp_port=proc.tcp_port,
            storage_config=storage_cfg,
        )
        await rebooted_node.start()

        # Update process records
        proc.node_id = rebooted_node.node_id.hex()
        proc.bound_udp_port = rebooted_node.bound_udp_port
        proc.bound_tcp_port = rebooted_node.bound_tcp_port
        proc.state = NodeLifecycleState.HEALTHY
        proc.restart_count += 1
        proc.last_heartbeat = time.time()

        self.nodes[name] = rebooted_node

        # Re-wire connections with active peers
        for peer_name, peer_node in self.nodes.items():
            if peer_name != name:
                rebooted_node.register_neighbor(
                    peer_node.node_id.hex(), peer_node.host, peer_node.bound_udp_port, peer_node.bound_tcp_port
                )
                peer_node.register_neighbor(
                    rebooted_node.node_id.hex(), rebooted_node.host, rebooted_node.bound_udp_port, rebooted_node.bound_tcp_port
                )

        await asyncio.sleep(0.15)
        logger.info(f"[CLUSTER REBOOTED] Node '{name}' successfully restored and rejoined cluster.")
        return rebooted_node

    async def rolling_restart(self, delay_between_nodes: float = 0.4) -> None:
        """Perform zero-downtime rolling reboot of every node across the cluster."""
        logger.info("[ROLLING RESTART] Commencing cluster rolling reboot...")
        node_names = list(self.processes.keys())
        for name in node_names:
            await self.kill_node(name, simulated_crash=False)
            await asyncio.sleep(0.1)
            await self.restart_node(name)
            await asyncio.sleep(delay_between_nodes)
        logger.info("[ROLLING RESTART] Rolling reboot complete across all cluster nodes.")

    async def stop(self) -> None:
        """Gracefully shut down all nodes in the cluster."""
        if not self._is_running:
            return

        self._is_running = False
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()

        for name, node in list(self.nodes.items()):
            try:
                await node.stop()
                if name in self.processes:
                    self.processes[name].state = NodeLifecycleState.STOPPED
            except Exception as e:
                logger.debug(f"Error stopping {name}: {e}")

        self.nodes.clear()
        logger.info(f"[CLUSTER STOP] Cluster '{self.config.cluster_name}' shut down.")

    def get_cluster_summary(self) -> Dict[str, Any]:
        """Aggregate comprehensive multi-node cluster status and health telemetry."""
        healthy_count = len([p for p in self.processes.values() if p.state == NodeLifecycleState.HEALTHY])
        crashed_count = len([p for p in self.processes.values() if p.state == NodeLifecycleState.CRASHED])
        stopped_count = len([p for p in self.processes.values() if p.state == NodeLifecycleState.STOPPED])

        leader_inst = self.leader
        leader_name = None
        leader_id = None
        if leader_inst:
            leader_id = leader_inst.node_id.hex()
            for n, inst in self.nodes.items():
                if inst == leader_inst:
                    leader_name = n
                    break

        return {
            "cluster_name": self.config.cluster_name,
            "topology": self.config.topology.value,
            "total_nodes": len(self.processes),
            "healthy_nodes": healthy_count,
            "crashed_nodes": crashed_count,
            "stopped_nodes": stopped_count,
            "leader_node_id": leader_id,
            "leader_name": leader_name,
            "uptime_seconds": time.time() - self._start_time if self._start_time > 0 else 0.0,
            "nodes": [p.to_dict() for p in self.processes.values()],
        }

    async def trigger_cluster_election(self, target_node_name: Optional[str] = None) -> Optional[MeshNode]:
        """Trigger leader election on a designated node or the first healthy node."""
        if target_node_name and target_node_name in self.nodes:
            target = self.nodes[target_node_name]
        elif self.nodes:
            target = next(iter(self.nodes.values()))
        else:
            return None

        await target.trigger_election()
        await asyncio.sleep(0.4)
        return self.leader

    def format_status_table(self) -> str:
        """Render a clean ASCII status table of all managed cluster nodes."""
        lines = [
            f"=== {self.config.cluster_name} ({self.config.topology.value}) ===",
            f"{'Name':<10} {'NodeID':<12} {'Address':<22} {'State':<10} {'Role':<10} {'Restarts':<8}",
            "-" * 74,
        ]
        for name, proc in self.processes.items():
            role = "👑 LEADER" if proc.is_leader else "FOLLOWER"
            addr = f"{proc.host}:{proc.udp_port}"
            lines.append(
                f"{name:<10} {proc.node_id[:8]:<12} {addr:<22} {proc.state.value:<10} {role:<10} {proc.restart_count:<8}"
            )
        lines.append("=" * 74)
        return "\n".join(lines)


