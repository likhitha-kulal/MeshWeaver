# MeshWeaver

A distributed, decentralized peer-to-peer compute mesh built with pure Python and `asyncio`.

MeshWeaver provides peer discovery via Kademlia DHT routing, decentralized gossip-based node health monitoring, load-balanced task scheduling, distributed MapReduce pipelines, tamper-resistant remote task execution over streaming TCP connections, circuit breaker resilience, multi-tier QoS priority queues, lease-based distributed consensus leader election, Raft replicated state machine log consensus with distributed mutual exclusion locking, high-availability consensus-driven job orchestration, and Raft snapshot state transfer (`InstallSnapshot` RPC).

---

## 🌟 Key Architecture & Features

1. **Consensus-Backed Distributed Job Orchestrator (`meshweaver.consensus_orchestrator`)**
   - High-availability distributed job lifecycle (`SUBMITTED` $\to$ `ASSIGNED` $\to$ `RUNNING` $\to$ `COMPLETED` / `FAILED` / `CANCELLED`).
   - Automated orphan task detection and failover re-assignment upon worker node disconnects.
   - Monotonic fencing tokens guaranteeing exactly-once execution safety.
   - Live cluster job status queries and real-time execution telemetry.

2. **Raft Snapshot State Transfer & Log Compaction (`meshweaver.raft_log`)**
   - `InstallSnapshot` RPC protocol streaming full state snapshots to lagging followers.
   - Fast state recovery and compaction boundary synchronization without log gap errors.
   - Dynamic cluster membership reconfiguration via consensus log entries.

3. **Replicated State Machine & Raft Log Consensus Engine (`meshweaver.raft_log`)**
   - 1-based term-indexed sequential commit log with log matching invariant verification.
   - Dynamic majority quorum commit advancement: $Q = \lfloor \frac{N}{2} \rfloor + 1$.
   - Atomic Compare-And-Swap (CAS), monotonic distributed counters, and batch transactions.
   - Distributed Lock Manager (DLM) with lease TTL and monotonic fencing tokens preventing zombie writes.
   - Follower log conflict detection, truncation, and automatic lag recovery.

4. **Distributed Consensus & Leader Election Engine (`meshweaver.leader_election`)**
   - Randomized lease-based leader election protocol with candidate term preemption.
   - Dynamic majority quorum calculation: $Q = \lfloor \frac{N}{2} \rfloor + 1$.
   - Periodic leader heartbeat lease renewals with fast follower lease timers.
   - Graceful leader failure detection and automatic cluster failover re-election.

5. **Multi-Tier Priority Task Queue & QoS Engine (`meshweaver.priority_queue`)**
   - 5 QoS Precedence Tiers: `CRITICAL` (0), `HIGH` (1), `NORMAL` (2), `LOW` (3), `BACKGROUND` (4).
   - Starvation-free dynamic aging promotion: $P_{\text{eff}} = P_{\text{base}} - \frac{\text{wait}}{T_{\text{aging}}} - \text{urgency}$.
   - Impending deadline urgency boosting with instant preemption for time-sensitive queries.
   - Asynchronous `PriorityDispatcher` worker pool and live QoS telemetry.

6. **Circuit Breaker Fault Isolation & Resilience (`meshweaver.circuit_breaker`)**
   - Three-state resilience engine (`CLOSED`, `OPEN`, `HALF_OPEN`) preventing cascading cluster failures.
   - Automatic node isolation upon reaching configurable failure thresholds.
   - Proactive `HALF_OPEN` health probe trials after configurable recovery timeouts.
   - Fine-grained exception discrimination protecting transport circuits from user-level exceptions.

7. **Distributed MapReduce & Aggregation Engine (`meshweaver.map_reduce`)**
   - Full distributed Map $\to$ Shuffle/Partition $\to$ Reduce compute engine (`mesh.map_reduce`).
   - Hierarchical $O(\log_b N)$ parallel tree reduction (`mesh.tree_reduce`) for associative operations.
   - Fine-grained stage performance telemetry (Map, Shuffle, Reduce durations & throughput).

8. **Multi-Stage Task Pipeline / DAG Engine (`meshweaver.pipeline`)**
   - Composable multi-stage data processing graphs (`mesh.create_pipeline`).
   - Parallel item-wise worker dispatch or dataset transformations per stage.
   - Comprehensive stage-by-stage execution profiling and error capture.

9. **Distributed Parallel Batch Execution (`meshweaver.batch_executor`)**
   - Parallel MapReduce-style compute engine (`mesh.map`) with input sequence chunking and concurrency throttling.
   - Asynchronous streaming generator (`map_unordered`) for continuous data processing pipelines.
   - Detailed performance telemetry capturing execution duration and cluster throughput (items/s).

10. **Intelligent Load-Balanced Task Scheduler & Failover (`meshweaver.scheduler`)**
    - Dynamic worker selection algorithms: `LEAST_LOADED`, `ROUND_ROBIN`, `POWER_OF_TWO_RANDOM`, and `LOCAL_FIRST`.
    - Automated worker failover and retry loop with exponential backoff and failed node blacklisting.
    - Live composite load scoring: $S = 0.6 \times \text{CPU}\% + 0.4 \times \text{RAM}\% + 5.0 \times \text{in\_flight}$.

11. **160-Bit Kademlia DHT Routing (`meshweaver.routing_table`, `meshweaver.kbucket`)**
    - 160-bit SHA-1 address space with standard XOR metric distance calculations.
    - 160 K-Buckets ($k=20$) with least-recently-seen (LRU) replacement caches.
    - `FIND_NODE` RPC protocol over UDP for nearest-neighbor contact discovery.
    - Dynamic network bootstrapping.

12. **Gossip Health & Load Monitoring (`meshweaver.gossip`)**
    - Periodic UDP heartbeat broadcasts with resource telemetry (CPU% and RAM%).
    - Dynamic peer table updates and stale/dead node timeout eviction.
    - Intelligent least-loaded worker selection for distributed task scheduling.

13. **DHT Task Result Memoization (`meshweaver.task_cache`)**
    - Deterministic SHA-256 caching of task bytecodes and argument combinations in the Kademlia DHT.
    - Transparent cache hit bypass to avoid redundant remote executions.

14. **Secure Remote Task Execution Engine (`meshweaver.task_serializer`)**
    - Dynamic serialization and deserialization of arbitrary Python callables using `cloudpickle`.
    - `TaskEnvelope` encapsulation with SHA-256 cryptographic checksums to detect and reject corrupted or tampered payloads prior to deserialization.
    - Support for both synchronous functions and `async def` coroutines.
    - Remote error diagnostics and stack trace propagation via `RemoteExecutionError`.

---

## 📦 Project Structure

```
MeshWeaver/
├── node.py                     # CLI entry point
├── README.md                   # Project documentation
├── docs/
│   ├── PROTOCOL_SPEC.md        # Wire protocol and framing spec
│   ├── SCHEDULER_SPEC.md       # Load balancing & failover spec
│   ├── MAPREDUCE_SPEC.md       # MapReduce & Pipeline DAG architecture
│   ├── CIRCUIT_BREAKER_SPEC.md # Circuit Breaker state machine & resilience spec
│   ├── PRIORITY_SCHEDULER_SPEC.md # QoS Priority queue & starvation aging spec
│   ├── LEADER_ELECTION_SPEC.md # Distributed consensus & leader election spec
│   ├── RAFT_REPLICATION_SPEC.md # Raft Log Replication & Replicated State Machine spec
│   └── CONSENSUS_ORCHESTRATOR_SPEC.md # Consensus Job Orchestrator & Snapshot Transfer spec
├── examples/
│   ├── distributed_word_count.py # Distributed MapReduce word count benchmark
│   ├── monte_carlo_pi.py         # Distributed Monte Carlo Pi estimation
│   ├── resilient_cluster_demo.py # Cluster fault tolerance & circuit breaker demo
│   ├── fault_injection_benchmark.py # Fault injection & stress benchmark suite
│   ├── priority_qos_demo.py      # Priority QoS & starvation aging demonstration
│   ├── leader_election_demo.py   # Distributed consensus & failover demonstration
│   ├── replicated_state_demo.py  # Replicated state machine & DLM demonstration
│   └── consensus_job_orchestration_demo.py # Consensus Job Orchestration & Snapshot demo
├── meshweaver/
│   ├── __init__.py             # Public package exports (v0.5.0)
│   ├── models.py               # NodeID, NodeInfo, LogEntry, AppendEntries, InstallSnapshot, ConsensusJob
│   ├── kbucket.py              # K-Bucket contact storage with LRU eviction
│   ├── routing_table.py        # 160-bit Kademlia routing table
│   ├── node_lookup.py          # Iterative Kademlia FIND_NODE lookup
│   ├── dht_storage.py          # Distributed key/value store & find_value RPCs
│   ├── gossip.py               # Gossip protocol and node load monitoring
│   ├── networking.py           # UDP datagram protocol and TCP framing
│   ├── task_serializer.py      # Cloudpickle serialization & execution engine
│   ├── scheduler.py            # Intelligent task scheduler & failover engine
│   ├── circuit_breaker.py      # Circuit Breaker fault isolation & state machine
│   ├── priority_queue.py       # Priority Task Queue & QoS Dispatcher engine
│   ├── leader_election.py      # Distributed consensus & leader election engine
│   ├── raft_log.py             # Raft Log Replication, State Machine & Snapshot Transfer
│   ├── consensus_orchestrator.py # Consensus-Backed Distributed Job Orchestrator
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
│       ├── test_circuit_breaker.py
│       ├── test_priority_queue.py
│       ├── test_leader_election.py
│       ├── test_raft_models.py
│       ├── test_raft_log.py
│       ├── test_replicated_state_machine.py
│       ├── test_raft_replication.py
│       ├── test_snapshot_transfer.py
│       ├── test_consensus_orchestrator.py
│       ├── test_task_cache.py
│       ├── test_batch_executor.py
│       ├── test_map_reduce.py
│       ├── test_pipeline.py
│       ├── test_dht_network.py
│       ├── test_cluster_scheduler.py
│       ├── test_cluster_pipeline.py
│       ├── test_cluster_circuit_breaker.py
│       ├── test_cluster_priority.py
│       ├── test_cluster_election.py
│       ├── test_cluster_state_replication.py
│       └── test_cluster_orchestrator.py
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

### 3. Run Consensus Job Orchestrator Demo
```bash
python examples/consensus_job_orchestration_demo.py
```

### 4. Run Replicated State Machine & Distributed Locking Demo
```bash
python examples/replicated_state_demo.py
```

### 5. Run Leader Election Consensus Demo
```bash
python examples/leader_election_demo.py
```

---

## 🧪 Running the Test Suite

Run the full unit and integration test suite:

```bash
python -m pytest
```


<!-- Benchmark Checkpoint: Monte Carlo Pi 1M samples verified -->
