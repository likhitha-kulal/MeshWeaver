# MeshWeaver Chaos Engineering & Fault Injection Specification (Week 4 Day 5)

## 1. Overview
The MeshWeaver Chaos & Reliability Engine (`meshweaver.chaos`) provides synthetic fault injection capabilities for validating distributed cluster resilience under adverse real-world network and hardware failure scenarios. It intercepts communications at the datagram and transport layer, enabling non-invasive testing of Raft leader failovers, 2PC rollbacks, dynamic topology reconfigurations, and Byzantine fault handling.

---

## 2. Chaos Injection Architecture

```
                       [ MeshNode Layer ]
                                │
                 _send_datagram(msg, host, port)
                                │
                                ▼
               ┌─────────────────────────────────┐
               │          ChaosEngine            │
               │   (Active Network Simulator)    │
               └──────────────┬──────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│ NetworkPartition │ │  PacketDropRule  │ │ LatencyInjector  │
│ - Split-Brain    │ │ - Drop Rate %    │ │ - Min/Max Delay  │
│ - Bi/Unidir      │ │ - Filter by Type │ │ - Jitter Jitter  │
└────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘
         │                    │                    │
         ▼                    ▼                    ▼
   [ Is Blocked? ]      [ Drop Random? ]     [ Schedule Async ]
   ├── YES ──► DROP     ├── YES ──► DROP     └── NO ──► Delay ms
   └── NO  ──► PASS     └── NO  ──► PASS                └──► Transmit
```

---

## 3. Supported Fault Injection Modes

### 3.1 Network Partition & Split-Brain Simulation
Simulates complete or partial communication severance between node groups:
- **Majority vs. Minority Split**: Divides $N$ nodes into quorum ($>\frac{N}{2}$) and isolated sub-clusters. Majority continues committing Raft and 2PC mutations, while minority operations fail cleanly without data corruption.
- **Asymmetric Partition**: Unidirectional packet blocking (e.g. Node A can send to Node B, but Node B responses are dropped).
- **Node Isolation**: Completely severs a node from all incoming and outgoing cluster datagrams (`isolate_node`).

### 3.2 Packet Drop & Filter Rules (`PacketDropRule`)
Configurable random and deterministic datagram loss:
- **Global Packet Loss**: Drops datagrams with probability $P \in [0.0, 1.0]$.
- **Targeted Message Filtering**: Selectively drops only specific `MessageType` entries (e.g. drop only `RAFT_HEARTBEAT` to force election cascades, or drop `TX_COMMIT` to test transaction coordinator timeouts).
- **Sender/Recipient Rules**: Drops traffic only between designated source/target hex NodeIDs.

### 3.3 Latency & Jitter Injection (`LatencyJitterInjector`)
Simulates wide-area network (WAN) latency, buffering delays, and cross-datacenter jitter:
- **Minimum & Maximum Delay**: Adds bounded synthetic delays $D \in [D_{\min}, D_{\max}]$ milliseconds.
- **Jitter Distribution**: Applies randomized variance $\pm J$ ms to simulate TCP head-of-line blocking and packet reordering.
- **Non-blocking Scheduling**: Injected delays use asynchronous timers (`loop.call_later`) without starving the event loop.

### 3.4 Byzantine Payload Tampering & Envelope Defenses (`TaskEnvelope`)
Validates cluster defense against corrupted or malicious serialized payloads:
- **Cryptographic SHA-256 Envelopes**: All remote task payloads are wrapped with SHA-256 checksums.
- **Bit-Flip / Corruption Injector**: Intentionally modifies bytes in-flight to verify that receiving nodes reject tampered execution requests with `RemoteExecutionError` rather than executing corrupted instructions.

---

## 4. Self-Healing & Convergence Lifecycle

```
[ Active Partition / Drop Rules ]
               │
      heal_all() / heal_chaos()
               │
               ▼
┌───────────────────────────────┐
│     Tear Down All Filters     │
│  - Clear NetworkPartition     │
│  - Clear PacketDropRule       │
│  - Reset Latency Injectors    │
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│     Cluster Convergence       │
│  - Raft Log Sync (Catchup)    │
│  - Gossip Peer Heartbeats     │
│  - 2PC In-Flight Resolution   │
└───────────────────────────────┘
```

When faults are cleared, surviving leaders automatically transmit missing Raft log entries via `AppendEntries` or `InstallSnapshot` RPCs, returning the cluster to consistent state without manual operator intervention.

---

## 5. Telemetry & Metrics Snapshot (`ChaosMetrics`)

The Chaos Engine exposes operational telemetry for verification:

| Metric Field | Type | Description |
| :--- | :--- | :--- |
| `total_packets_inspected` | `int` | Total number of UDP datagrams evaluated by filter chain |
| `total_packets_dropped` | `int` | Count of packets dropped due to partition or loss rules |
| `total_packets_delayed` | `int` | Count of packets delayed via latency injector |
| `total_packets_corrupted` | `int` | Count of simulated Byzantine corruptions injected |
| `total_flaky_rpc_aborted` | `int` | Synthetic RPC timeouts triggered on flaky workers |
| `total_latency_injected_ms` | `float` | Cumulative artificial latency injected into network |
| `active_partitions_count` | `int` | Number of currently active network partition rules |

---

## 6. Programmatic API Reference

```python
from meshweaver.node import MeshNode

node = MeshNode(host="127.0.0.1", udp_port=18000)
await node.start()

# 1. Inject artificial latency
node.inject_latency(min_ms=10.0, max_ms=30.0, jitter_ms=5.0)

# 2. Inject random packet drop
node.set_packet_loss(0.15)  # 15% drop rate

# 3. Create bidirectional partition between two groups
node.create_partition("split_alpha", group_a={node_id_1, node_id_2}, group_b={node_id_3})

# 4. Self-heal
node.heal_chaos()

# 5. Inspect metrics
metrics = node.get_chaos_metrics()
print(f"Packets dropped: {metrics['total_packets_dropped']}")
```
