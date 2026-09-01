# MeshWeaver

A distributed, decentralized peer-to-peer compute mesh built with pure Python and `asyncio`.

MeshWeaver provides peer discovery via Kademlia DHT routing, decentralized gossip-based node health monitoring, load-balanced task scheduling, distributed MapReduce pipelines, and tamper-resistant remote task execution over streaming TCP connections.

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

4. **Distributed MapReduce & Aggregation Engine (`meshweaver.map_reduce`)**
   - Full distributed Map $\to$ Shuffle/Partition $\to$ Reduce compute engine (`mesh.map_reduce`).
   - Hierarchical $O(\log_b N)$ parallel tree reduction (`mesh.tree_reduce`) for associative operations.
   - Fine-grained stage performance telemetry (Map, Shuffle, Reduce durations & throughput).

5. **Multi-Stage Task Pipeline / DAG Engine (`meshweaver.pipeline`)**
   - Composable multi-stage data processing graphs (`mesh.create_pipeline`).
   - Parallel item-wise worker dispatch or dataset transformations per stage.
   - Comprehensive stage-by-stage execution profiling and error capture.

6. **Distributed Parallel Batch Execution (`meshweaver.batch_executor`)**
   - Parallel MapReduce-style compute engine (`mesh.map`) with input sequence chunking and concurrency throttling.
   - Asynchronous streaming generator (`map_unordered`) for continuous data processing pipelines.
   - Detailed performance telemetry capturing execution duration and cluster throughput (items/s).

7. **DHT Task Result Memoization (`meshweaver.task_cache`)**
   - Deterministic SHA-256 caching of task bytecodes and argument combinations in the Kademlia DHT.
   - Transparent cache hit bypass to avoid redundant remote executions.

8. **Secure Remote Task Execution Engine (`meshweaver.task_serializer`)**
   - Dynamic serialization and deserialization of arbitrary Python callables using `cloudpickle`.
   - `TaskEnvelope` encapsulation with SHA-256 cryptographic checksums to detect and reject corrupted or tampered payloads prior to deserialization.
   - Support for both synchronous functions and `async def` coroutines.
   - Remote error diagnostics and stack trace propagation via `RemoteExecutionError`.

9. **Network Transport Layer (`meshweaver.networking`)**
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
│   ├── SCHEDULER_SPEC.md       # Load balancing & failover spec
│   └── MAPREDUCE_SPEC.md       # MapReduce & Pipeline DAG architecture
├── examples/
│   ├── distributed_word_count.py # Distributed MapReduce word count benchmark
│   └── monte_carlo_pi.py         # Distributed Monte Carlo Pi estimation
├── meshweaver/
│   ├── __init__.py             # Public package exports (v0.3.4)
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
│   ├── map_reduce.py           # Distributed MapReduce & tree_reduce engine
│   ├── pipeline.py             # Multi-stage computation DAG pipeline
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
│       ├── test_map_reduce.py
│       ├── test_pipeline.py
│       ├── test_dht_network.py
│       ├── test_cluster_scheduler.py
│       └── test_cluster_pipeline.py
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

### 3. Run Distributed MapReduce Word Count Demo

```bash
python node.py --host 127.0.0.1 --port 9020 --bootstrap-host 127.0.0.1 --bootstrap-port 9000 --mapreduce-demo
```

### 4. Run Multi-Stage Pipeline Demo

```bash
python node.py --host 127.0.0.1 --port 9030 --bootstrap-host 127.0.0.1 --bootstrap-port 9000 --pipeline-demo
```

### 5. Run Real-World Example Benchmarks

```bash
# Distributed Word Count MapReduce:
python examples/distributed_word_count.py

# Distributed Monte Carlo Pi Estimation (1 Million points):
python examples/monte_carlo_pi.py
```

---

## 🧪 Running the Test Suite

Run the full unit and integration test suite:

```bash
python -m unittest discover -s meshweaver/tests
```