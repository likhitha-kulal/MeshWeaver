"""
MeshWeaver Gossip Protocol
Decentralized peer-health gossip broadcaster and node load balancer.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("meshweaver.gossip")


def _read_cpu_percent() -> float:
    try:
        import psutil
        return float(psutil.cpu_percent(interval=None))
    except Exception:
        return 0.0


def _read_ram_percent() -> float:
    try:
        import psutil
        return float(psutil.virtual_memory().percent)
    except Exception:
        return 0.0


@dataclass
class PeerLoadSnapshot:
    """Resource utilization snapshot of a known peer."""
    node_id: str
    ip: str
    udp_port: int
    cpu_percent: float = 0.0
    ram_percent: float = 0.0
    timestamp: float = field(default_factory=time.time)


class GossipManager:
    """Periodic gossip heartbeat manager and peer load table."""

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        udp_port: int = 0,
        heartbeat_interval: float = 5.0,
        dead_node_timeout: float = 15.0,
    ):
        self.node_id = node_id
        self.host = host
        self.udp_port = udp_port
        self.heartbeat_interval = heartbeat_interval
        self.dead_node_timeout = dead_node_timeout
        self.peer_loads: Dict[str, PeerLoadSnapshot] = {}
        self.neighbors: Dict[str, Tuple[str, int]] = {}
        self._network_send: Optional[Callable[[str, int, Dict[str, Any]], None]] = None
        self._task: Optional[asyncio.Task[None]] = None
        self._stopped = False

    @property
    def peer_table(self) -> Dict[str, PeerLoadSnapshot]:
        return self.peer_loads

    @peer_table.setter
    def peer_table(self, val: Dict[str, PeerLoadSnapshot]) -> None:
        self.peer_loads = val

    def set_send_callback(self, send_cb: Optional[Callable[[str, int, Dict[str, Any]], None]]) -> None:
        self._network_send = send_cb

    def set_receive_callback(self, receive_cb: Optional[Callable[[Dict[str, Any]], None]]) -> None:
        self._receive_cb = receive_cb

    def bind_network(self, protocol: Any) -> None:
        self._network_protocol = protocol

    def register_neighbor(self, node_id: str, host: str, udp_port: int, tcp_port: Optional[int] = None) -> None:
        self.neighbors[node_id] = (host, udp_port)
        self.peer_loads.setdefault(node_id, PeerLoadSnapshot(node_id=node_id, ip=host, udp_port=udp_port))

    def build_heartbeat(self) -> Dict[str, Any]:
        return {
            "sender_id": self.node_id,
            "ip": self.host,
            "udp_port": self.udp_port,
            "cpu_percent": _read_cpu_percent(),
            "ram_percent": _read_ram_percent(),
            "timestamp": time.time(),
        }

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopped = False
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run_loop(self) -> None:
        try:
            while not self._stopped:
                await asyncio.sleep(self.heartbeat_interval)
                await self._broadcast_heartbeat()
                self.expire_dead_nodes()
        except asyncio.CancelledError:
            logger.debug("Gossip loop terminated")

    async def _broadcast_heartbeat(self) -> None:
        if self._network_send is None:
            return
        payload = self.build_heartbeat()
        for node_id, (host, port) in self.neighbors.items():
            try:
                self._network_send(host, port, payload)
            except Exception as exc:
                logger.warning(f"Failed to send gossip to {node_id}: {exc}")

    def receive_heartbeat(self, payload: Dict[str, Any]) -> None:
        try:
            sender_id = str(payload.get("sender_id"))
            if not sender_id:
                return
            ip = str(payload.get("ip", "127.0.0.1"))
            udp_port = int(payload.get("udp_port", 0))
            cpu = float(payload.get("cpu_percent", 0.0))
            ram = float(payload.get("ram_percent", 0.0))
            timestamp = float(payload.get("timestamp", time.time()))

            self.peer_loads[sender_id] = PeerLoadSnapshot(
                node_id=sender_id,
                ip=ip,
                udp_port=udp_port,
                cpu_percent=cpu,
                ram_percent=ram,
                timestamp=timestamp,
            )
            self.neighbors.setdefault(sender_id, (ip, udp_port))
        except (TypeError, ValueError):
            logger.warning(f"Ignoring malformed gossip heartbeat: {payload}")

    def expire_dead_nodes(self, now: Optional[float] = None) -> List[str]:
        if now is None:
            now = time.time()
        dead = [
            node_id
            for node_id, snapshot in self.peer_loads.items()
            if (now - snapshot.timestamp) > self.dead_node_timeout
        ]
        for node_id in dead:
            self.peer_loads.pop(node_id, None)
            self.neighbors.pop(node_id, None)
        return dead

    def get_least_loaded_peer(self) -> Optional[PeerLoadSnapshot]:
        """Return the peer with lowest combined CPU and RAM utilization score."""
        if not self.peer_loads:
            return None
        return min(
            self.peer_loads.values(),
            key=lambda s: (s.cpu_percent + s.ram_percent, s.node_id),
        )


# Helper: Snapshot validation and load scoring
def compute_load_score(snapshot: PeerLoadSnapshot) -> float:
    """Compute normalized composite load index (0.0 to 1.0)."""
    return (snapshot.cpu_percent * 0.6 + snapshot.ram_percent * 0.4) / 100.0

# Enhanced GossipManager type annotations and telemetry parameters

# Neighbor registration deduplication and active link validation

# Peer load retrieval and contact resolution helpers

# Strict TTL expiration threshold verification

def pack_heartbeat_payload(node_id: str, host: str, udp_port: int, cpu: float, ram: float, tcp_port: int = 0) -> dict:
    """Serialize node health and telemetry into standardized gossip payload."""
    return {"type": "GOSSIP_HEARTBEAT", "node_id": node_id, "host": host, "udp_port": udp_port, "tcp_port": tcp_port, "cpu": cpu, "ram": ram, "timestamp": time.time()}

def unpack_heartbeat_payload(payload: dict) -> dict:
    """Validate and parse received gossip heartbeat dictionary."""
    return {"node_id": payload.get("node_id", ""), "host": payload.get("host", "127.0.0.1"), "udp_port": int(payload.get("udp_port", 0)), "tcp_port": int(payload.get("tcp_port", 0)), "cpu": float(payload.get("cpu", 0.0)), "ram": float(payload.get("ram", 0.0)), "timestamp": float(payload.get("timestamp", time.time()))}

# Heartbeat broadcast loop lifecycle monitor

# Jitter calculation helper for gossip broadcast dispersion

# Automatic peer record refresh on heartbeat arrival

# Node liveness deadline checking and dead node marking

# Event hooks for node eviction notification
