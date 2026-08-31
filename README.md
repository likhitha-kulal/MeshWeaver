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

3. **Intelligent Load-Balanced Task Scheduler & Failover (`meshweaver.scheduler`)**
   - Dynamic worker selection algorithms: `LEAST_LOADED`, `ROUND_ROBIN`, `POWER_OF_TWO_RANDOM`, and `LOCAL_FIRST`.
   - Automated worker failover and retry loop with exponential backoff and failed node blacklisting.
   - Live composite load scoring: $S = 0.6 \times \text{CPU}\% + 0.4 \times \text{RAM}\% + 5.0 \times \text{in\_flight}$.

4. **Distributed Parallel Batch Execution (`meshweaver.batch_executor`)**
   - Parallel MapReduce-style compute engine (`mesh.map`) with input sequence chunking and concurrency throttling.
   - Asynchronous streaming generator (`map_unordered`) for continuous data processing pipelines.
   - Detailed performance telemetry capturing execution duration and cluster throughput (items/s).

5. **DHT Task Result Memoization (`meshweaver.task_cache`)**
   - Deterministic SHA-256 caching of task bytecodes and argument combinations in the Kademlia DHT.
   - Transparent cache hit bypass to avoid redundant remote executions.

6. **Secure Remote Task Execution Engine (`meshweaver.task_serializer`)**
   - Dynamic serialization and deserialization of arbitrary Python callables (functions, closures, lambdas) using `cloudpickle`.
   - `TaskEnvelope` encapsulation with SHA-256 cryptographic checksums to detect and reject corrupted or tampered payloads prior to deserialization.
   - Support for both synchronous functions and `async def` coroutines.
   - Remote error diagnostics and stack trace propagation via `RemoteExecutionError`.

7. **Network Transport Layer (`meshweaver.networking`)**
   - `UDPNodeProtocol`: Low-latency, non-blocking UDP datagram messaging for PING/PONG heartbeats, gossip, and DHT lookups.
   - `TCPTaskServer` & `TCPTaskClient`: Length-prefixed framed binary stream transport for task dispatch and result reception.

---

## 📦 Project Structure

```
MeshWeaver/
├── node.py                     # CLI entry point
├── README.md                   # Project documentation
├── docs/
│   ├── PROTOCOL_SPEC.md        # Wire protocol and framing spec
│   └── SCHEDULER_SPEC.md       # Load balancing & failover spec
├── meshweaver/
│   ├── __init__.py             # Public package exports (v0.3.0)
│   ├── models.py               # NodeID, NodeInfo, Message, TaskEnvelope, TaskResult
│   ├── kbucket.py              # K-Bucket contact storage with LRU eviction
│   ├── routing_table.py        # 160-bit Kademlia routing table
│   ├── node_lookup.py          # Iterative Kademlia FIND_NODE lookup
│   ├── dht_storage.py          # Distributed key/value store & find_value RPCs
│   ├── gossip.py               # Gossip protocol and node load monitoring
│   ├── networking.py           # UDP datagram protocol and TCP framing
│   ├── task_serializer.py      # Cloudpickle serialization & execution engine
│   ├── scheduler.py            # Intelligent task scheduler & failover engine
│   ├── task_cache.py           # DHT-backed result memoization & caching
│   ├── batch_executor.py       # Distributed parallel map & batch runner
│   ├── node.py                 # MeshNode coordinator
│   └── tests/                  # Unit and integration test suite
│       ├── __init__.py
│       ├── test_node_id.py
│       ├── test_kbucket.py
│       ├── test_routing_table.py
│       ├── test_task_serializer.py
│       ├── test_networking.py
│       ├── test_gossip.py
│       ├── test_scheduler.py
│       ├── test_task_cache.py
│       ├── test_batch_executor.py
│       ├── test_week2.py
│       └── test_week3.py
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

### 3. Run Distributed Parallel Batch Map

```bash
python node.py --host 127.0.0.1 --port 9020 --bootstrap-host 127.0.0.1 --bootstrap-port 9000 --batch-demo --scheduler-policy least_loaded
```

### 4. Run DHT Memoized Cached Compute Demo

```bash
python node.py --host 127.0.0.1 --port 9030 --bootstrap-host 127.0.0.1 --bootstrap-port 9000 --cache-demo
```

---

## 🧪 Running the Test Suite

Run all 52 unit and integration tests:

```bash
python -m unittest discover -s meshweaver/tests
```