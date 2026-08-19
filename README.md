# MeshWeaver

A distributed, decentralized peer-to-peer compute mesh built with pure Python and `asyncio`.

MeshWeaver provides peer discovery via Kademlia DHT routing, decentralized gossip-based node health monitoring, and tamper-resistant remote task execution over streaming TCP connections.

---

## 🌟 Key Architecture & Features

1. **160-Bit Kademlia DHT Routing (`meshweaver.routing_table`, `meshweaver.kbucket`)**
   - 160-bit SHA-1 address space with standard XOR metric distance calculations.
   - 160 K-Buckets ($k=20$) with least-recently-seen (LRU) replacement caches.
   - `FIND_NODE` RPC protocol over UDP for nearest-neighbor contact discovery.
   - Dynamic network bootstrapping.

2. **Gossip Health & Load Monitoring (`meshweaver.gossip`)**
   - Periodic UDP heartbeat broadcasts with resource telemetry (CPU% and RAM%).
   - Dynamic peer table updates and stale/dead node timeout eviction.
   - Intelligent least-loaded worker selection for distributed task scheduling.

3. **Secure Remote Task Execution Engine (`meshweaver.task_serializer`)**
   - Dynamic serialization and deserialization of arbitrary Python callables (functions, closures, lambdas) using `cloudpickle`.
   - `TaskEnvelope` encapsulation with SHA-256 cryptographic checksums to detect and reject corrupted or tampered payloads prior to deserialization.
   - Support for both synchronous functions and `async def` coroutines.
   - Remote error diagnostics and stack trace propagation via `RemoteExecutionError`.

4. **Network Transport Layer (`meshweaver.networking`)**
   - `UDPNodeProtocol`: Low-latency, non-blocking UDP datagram messaging for PING/PONG heartbeats, gossip, and DHT lookups.
   - `TCPTaskServer` & `TCPTaskClient`: Length-prefixed framed binary stream transport for task dispatch and result reception.

---

## 📦 Project Structure

```
MeshWeaver/
├── node.py                     # CLI entry point
├── README.md                   # Project documentation
├── meshweaver/
│   ├── __init__.py             # Public package exports
│   ├── models.py               # NodeID, NodeInfo, Message, TaskEnvelope, TaskResult
│   ├── kbucket.py              # K-Bucket contact storage with LRU eviction
│   ├── routing_table.py        # 160-bit Kademlia routing table
│   ├── gossip.py               # Gossip protocol and node load monitoring
│   ├── networking.py           # UDP datagram protocol and TCP framing
│   ├── task_serializer.py      # Cloudpickle serialization & execution engine
│   ├── node.py                 # MeshNode coordinator
│   └── tests/                  # Unit and integration test suite
│       ├── __init__.py
│       ├── test_node_id.py
│       ├── test_kbucket.py
│       ├── test_routing_table.py
│       ├── test_task_serializer.py
│       ├── test_networking.py
│       └── test_gossip.py
```

---

## 🚀 Quick Start

### 1. Start a Peer Node

```bash
python node.py --host 127.0.0.1 --port 9000
```

### 2. Join an Existing Mesh Network (Bootstrap)

```bash
python node.py --host 127.0.0.1 --port 9010 --bootstrap-host 127.0.0.1 --bootstrap-port 9000
```

### 3. Query Nearest Nodes (DHT Lookup)

```bash
python node.py --host 127.0.0.1 --port 9020 --ping-host 127.0.0.1 --ping-port 9000 --find-node <40-char-hex-node-id>
```

### 4. Submit Remote Tasks

```bash
python node.py --host 127.0.0.1 --port 9030 --task-target-port 9001 --demo-task
```

---

## 🧪 Running the Test Suite

Run all unit and integration tests:

```bash
python -m unittest discover -s meshweaver/tests
```