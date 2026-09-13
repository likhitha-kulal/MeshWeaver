# MeshWeaver Enterprise Cluster Architecture & Multi-Node Deployment Guide (v1.0.0)

## 1. Executive Summary & Overview
MeshWeaver is a modular, high-throughput, crash-resilient peer-to-peer distributed compute mesh engine written in Python 3.10+. It unifies decentralized discovery (Kademlia DHT), priority-aware load-balanced job scheduling, Raft distributed consensus, Write-Ahead Log (WAL) persistence, Two-Phase Commit (2PC) ACID transactions, distributed synchronization barriers, and synthetic chaos fault injection into a cohesive, zero-dependency distributed runtime.

---

## 2. Layered Subsystems Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          APPLICATION & CLI LAYER                            │
│  MeshNode CLI  │  Interactive Demos  │  Batch Map/Reduce  │  Pipelines      │
├─────────────────────────────────────────────────────────────────────────────┤
│                    RELIABILITY & FAULT INJECTION LAYER                      │
│  ChaosEngine   │  PacketDropRule     │  LatencyInjector   │  Byzantine Guard│
├─────────────────────────────────────────────────────────────────────────────┤
│                    TRANSACTION & SYNCHRONIZATION LAYER                      │
│  2PC OCC Coordinator │ Distributed Barriers │ Semaphores │ Countdown Latches│
├─────────────────────────────────────────────────────────────────────────────┤
│                    PERSISTENCE & DURABILITY (WAL) LAYER                     │
│  WALEngine     │  Segment Files (.wal)│ CRC32 Integrity   │ Crash Recovery  │
├─────────────────────────────────────────────────────────────────────────────┤
│                    DISTRIBUTED CONSENSUS (RAFT) LAYER                       │
│  Leader Election │ Log Replication   │ State Machine (RSM)│ Snapshot Sync   │
├─────────────────────────────────────────────────────────────────────────────┤
│                    TASK SCHEDULING & COMPUTE DISPATCH                       │
│  Priority Queue│ Token Bucket Shedder│ Circuit Breakers   │ Batch Executor  │
├─────────────────────────────────────────────────────────────────────────────┤
│                    PEER ROUTING & MEMBERSHIP (DHT/GOSSIP)                   │
│  Kademlia Routing Table │ K-Buckets  │ Gossip Heartbeats  │ Cluster Runner  │
├─────────────────────────────────────────────────────────────────────────────┤
│                    ASYNCHRONOUS NETWORKING & TRANSPORT                      │
│  UDP Datagram Protocol (AsyncIO)     │  TCP Streaming TaskServer            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Supported Cluster Topologies

MeshWeaver supports configurable cluster topologies managed natively by `LocalClusterRunner`:

### 3.1 Full-Mesh (`ClusterTopology.FULL_MESH`)
- **Connectivity**: Every node maintains direct peer routing to all other $N - 1$ nodes.
- **Latency**: $O(1)$ single-hop dispatch for RPCs and consensus proposals.
- **Best For**: Low-latency clusters, consensus-heavy state machines ($N \le 64$ nodes).

### 3.2 Ring Topology (`ClusterTopology.RING`)
- **Connectivity**: Each node connects strictly to immediate predecessor and successor peers.
- **Latency**: $O(N/2)$ average hop count.
- **Best For**: Token-passing algorithms, distributed ring buffers, localized gossip propagation.

### 3.3 Star Topology (`ClusterTopology.STAR`)
- **Connectivity**: Central hub node connected to $N - 1$ leaf worker spokes.
- **Latency**: 1 hop to hub, 2 hops inter-spoke.
- **Best For**: Centralized job dispatching with decentralized worker pools.

### 3.4 Linear / Pipeline Topology (`ClusterTopology.LINEAR`)
- **Connectivity**: Sequential chain ($N_1 \leftrightarrow N_2 \leftrightarrow \dots \leftrightarrow N_k$).
- **Latency**: $O(K)$ stage progression.
- **Best For**: Multi-stage distributed stream processing pipelines and MapReduce workflows.

---

## 4. Multi-Node Process Supervision & Lifecycle Management

The `LocalClusterRunner` coordinates in-process node lifecycles with strict health state tracking:

```
                  ┌──────────────┐
                  │   STOPPED    │
                  └──────┬───────┘
                         │ runner.start()
                         ▼
                  ┌──────────────┐
       ┌──────────┤   HEALTHY    ├──────────┐
       │          └──────┬───────┘          │
       │                 │                  │
kill_node(crash)         │ Heartbeat Fail   │ rolling_restart()
       │                 ▼                  │
       │          ┌──────────────┐          │
       └─────────►│   CRASHED    │◄─────────┘
                  └──────┬───────┘
                         │ restart_node() (WAL Replay)
                         ▼
                  ┌──────────────┐
                  │   HEALTHY    │
                  └──────────────┘
```

### Key Supervisory Capabilities:
1. **Dynamic Node Spawning**: Spin up nodes dynamically into a running cluster with automatic neighbor peering.
2. **Cold-Boot Recovery**: Restores crashed nodes to their previous state machine snapshot by replaying disk `.wal` segments.
3. **Rolling Zero-Downtime Reboot**: Sequentially restarts nodes with configurable inter-node delay, preserving Raft quorum throughout the rolling reboot.
4. **Health Aggregation**: Live telemetry monitoring active leaders, memory usage, transaction metrics, and crash counts.

---

## 5. Production CLI & Configuration Reference

### 5.1 CLI Execution Flags

```bash
# Start a standalone production MeshNode with WAL persistence
python -m meshweaver.node --host 0.0.0.0 --port 8000 --tcp-port 9000 --storage-dir /var/data/meshweaver --wal-fsync periodic

# Bootstrap into an existing cluster
python -m meshweaver.node --host 0.0.0.0 --port 8002 --bootstrap-host 10.0.0.1 --bootstrap-port 8000

# Launch with active Chaos Monkey fault injection enabled
python -m meshweaver.node --port 8000 --chaos-mode
```

### 5.2 Python API Deployment Example

```python
from meshweaver.cluster_runner import ClusterConfig, LocalClusterRunner
from meshweaver.models import ClusterTopology

# Define production cluster configuration
config = ClusterConfig(
    cluster_name="prod_compute_mesh",
    node_count=5,
    host="127.0.0.1",
    base_udp_port=18000,
    base_tcp_port=19000,
    topology=ClusterTopology.FULL_MESH,
    data_dir="/var/meshweaver/data",
    enable_wal=True,
    auto_bootstrap=True,
)

# Initialize supervisor
runner = LocalClusterRunner(config)
nodes = await runner.start()

# Elect Raft leader
await runner.trigger_cluster_election()

# Display cluster status table
print(runner.format_status_table())
```
