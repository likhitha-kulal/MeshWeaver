# Raft Log Replication & Replicated State Machine Specification

## 1. Overview
MeshWeaver Replicated State Machine (`meshweaver.raft_log`) provides strong consistency, linearizable key-value storage, monotonic distributed locking (DLM), and consensus-driven commit logs over peer-to-peer compute nodes.

---

## 2. Core State Machine & Invariants

```
               [Client State Proposal]
                          │
                          ▼
               ┌──────────────────────┐
               │    Raft Leader Log   │
               │ (1-based index entry)│
               └──────────┬───────────┘
                          │
                 AppendEntries RPCs
                          │
            ┌─────────────┴─────────────┐
            ▼                           ▼
 ┌──────────────────────┐   ┌──────────────────────┐
 │    Follower 1 Log    │   │    Follower 2 Log    │
 └──────────┬───────────┘   └──────────┬───────────┘
            │                          │
            └─────────────┬────────────┘
                          ▼
            Majority Quorum Calculation
               Q = floor(N / 2) + 1
                          │
                          ▼
            [Commit Index Advancement]
                          │
                          ▼
             [Replicated State Machine]
      - Deterministic Key-Value Execution
      - Monotonic Fencing Token Locks
```

### Log Matching Invariant
If two logs contain an entry with the same index and term:
1. They store the same command.
2. Their logs are identical in all preceding entries up to that index.

---

## 3. Distributed Lock Manager (DLM) & Monotonic Fencing Tokens

To prevent split-brain zombies and delayed packet hazards in distributed execution, locks issue strictly monotonic fencing tokens $T_{\text{fence}} \in \mathbb{N}$:

$$\forall i > j \implies T_{\text{fence}}(i) > T_{\text{fence}}(j)$$

Storage engines verify $T_{\text{fence}} \ge T_{\text{last\_applied}}$ before processing any transactional writes.

---

## 4. Replicated State Commands
- `SET(key, value)`: Atomically set a key/value pair.
- `GET(key)`: Read current state.
- `DELETE(key)`: Remove key from state machine.
- `CAS(key, expected, new_value)`: Compare-and-Swap conditional mutation.
- `INCREMENT(key, delta)`: Monotonic distributed counter arithmetic.
- `LOCK_ACQUIRE(resource, ttl_seconds)`: Acquire mutual exclusion lease with fencing token.
- `LOCK_RELEASE(resource, fencing_token)`: Release lock verifying token ownership.
- `BATCH(operations)`: Atomic multi-command execution.
