# Consensus-Backed Job Orchestrator & Raft Snapshot Transfer Specification

## 1. Overview
The MeshWeaver Consensus Job Orchestrator (`meshweaver.consensus_orchestrator`) provides high-availability distributed job scheduling and task state replication on top of the Raft Replicated State Machine (`meshweaver.raft_log`).

It ensures:
1. **Fault-Tolerant Job Lifecycle**: Task submissions, worker assignments, completions, failures, and cancellations are replicated through Raft consensus entries.
2. **Orphan Task Recovery**: If an assigned worker node crashes or disconnects, the cluster leader detects the timeout and automatically re-queues and re-assigns the job with monotonic fencing tokens.
3. **Log Compaction & InstallSnapshot RPC**: When follower lag exceeds available log history, leaders stream full state snapshots (`InstallSnapshotRequest`) bringing followers into immediate sync.
4. **Dynamic Cluster Membership Reconfiguration**: Membership mutations (`MEMBERSHIP_CHANGE`) are serialized as log entries to safely resize the cluster without split-brain risk.

---

## 2. Distributed Job State Machine

```
              [Client submit_job()]
                        │
                        ▼
                (Status: SUBMITTED)
                        │
             Leader Scheduler Matching
                        │
                        ▼
                 (Status: ASSIGNED)
              - Designated worker ID
              - Monotonic fencing token
                        │
           ┌────────────┴────────────┐
           ▼                         ▼
   Worker Executes OK         Worker Error / Crash / Timeout
           │                         │
           ▼                         ▼
  (Status: COMPLETED)       Retry < Max Retries?
  - Serialized result                │
                             ┌───────┴───────┐
                             ▼               ▼
                            YES              NO
                             │               │
                             ▼               ▼
                    (Status: SUBMITTED) (Status: FAILED)
                    - Re-queue orphan   - Error captured
```

---

## 3. Raft InstallSnapshot Protocol

When a follower's `next_index` falls behind the leader's compacted snapshot boundary ($next\_index \le snapshot\_last\_index$):

1. **Leader**:
   - Emits `InstallSnapshotRequest(term, leader_id, last_included_index, last_included_term, data, done=True)`.
2. **Follower**:
   - Verifies $term \ge current\_term$.
   - Atomically replaces state machine state: `state_machine.import_state(data)`.
   - Compacts local log: `log.restore_snapshot(last_included_index, last_included_term)`.
   - Advances $commit\_index \leftarrow \max(commit\_index, last\_included\_index)$ and $last\_applied \leftarrow \max(last\_applied, last\_included\_index)$.
   - Replies `InstallSnapshotResponse(term, follower_id, success=True, match_index=last_included_index)`.
3. **Leader**:
   - Sets follower $match\_index \leftarrow last\_included\_index$, $next\_index \leftarrow match\_index + 1$.
   - Tally quorum commits.

---

## 4. Replicated Job Commands
- `JOB_SUBMIT`: Enqueues new job with priority, timeout, and max retry configuration.
- `JOB_ASSIGN`: Binds job to worker with unique monotonic fencing token.
- `JOB_COMPLETE`: Stores execution result and resolves client wait future.
- `JOB_FAIL`: Records error diagnostic and increments retry count (or marks FAILED).
- `JOB_CANCEL`: Aborts pending/running job.
- `MEMBERSHIP_CHANGE`: Dynamically joins or removes cluster nodes.
