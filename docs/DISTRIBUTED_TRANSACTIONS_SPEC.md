# Distributed 2-Phase Commit (2PC) ACID Transaction Specification

## 1. Overview
MeshWeaver Distributed 2PC ACID Transaction Coordinator (`meshweaver.transactions`) coordinates multi-key, multi-node atomic transactions with Optimistic Concurrency Control (OCC), monotonic fencing tokens, and automated distributed rollback on partition failure or lock conflicts.

---

## 2. 2-Phase Commit Protocol Architecture

```
                 [ Client Transaction Session ]
                               │
                tx = await begin_transaction()
                tx.set("k1", v1)
                tx.set("k2", v2)
                               │
                 Phase 1: PREPARE (Voting)
                               │
                               ▼
                ┌──────────────────────────────┐
                │    Transaction Coordinator   │
                │     (Optimistic Locking)     │
                └──────────────┬───────────────┘
                               │
                Broadcast TX_PREPARE Datagram
                               │
            ┌──────────────────┴──────────────────┐
            ▼                                     ▼
 ┌──────────────────────┐              ┌──────────────────────┐
 │    Participant 1     │              │    Participant 2     │
 │ - Check Key Locks    │              │ - Check Key Locks    │
 │ - Verify OCC Version │              │ - Verify OCC Version │
 │ - Vote: VOTE_COMMIT  │              │ - Vote: VOTE_COMMIT  │
 └──────────┬───────────┘              └──────────┬───────────┘
            │                                     │
            └──────────────────┬──────────────────┘
                               │
                     All Votes == VOTE_COMMIT?
                     ├── YES ──► Phase 2a: TX_COMMIT (Apply to WAL & State Machine)
                     └── NO  ──► Phase 2b: TX_ABORT  (Release Locks & Rollback State)
```

---

## 3. Transaction State Machine & Invariants

```
                ┌──────────────┐
                │   ACTIVE     │
                └──────┬───────┘
                       │
             prepare_transaction()
                       │
                       ▼
                ┌──────────────┐
       ┌────────┤  PREPARING   ├────────┐
       │        └──────────────┘        │
       │ All Votes Commit       Any Abort / Timeout
       ▼                                ▼
┌──────────────┐                 ┌──────────────┐
│  COMMITTED   │                 │   ABORTED    │
└──────────────┘                 └──────────────┘
```

### ACID Invariants:
1. **Atomicity**: Either all operations in the write-set are applied across all participant nodes, or none are applied.
2. **Consistency**: State machine updates preserve version monotonicity and strict schema invariant boundaries.
3. **Isolation**: 
   - `READ_UNCOMMITTED`: Snapshot read without lock acquisition.
   - `READ_COMMITTED`: Guaranteed reads of committed state only.
   - `SERIALIZABLE`: Strict two-phase locking (2PL) and OCC version validation preventing phantom reads and write skew.
4. **Durability**: Upon phase 2 commit, coordinator and participants log `TX_MARKER` to persistent Write-Ahead Log (WAL) before acknowledging client.

---

## 4. Optimistic Concurrency Control (OCC) & Fencing Tokens

To eliminate distributed deadlock hazards, MeshWeaver uses OCC with generation fencing tokens $T_{\text{fence}} \in \mathbb{N}$:

$$\forall \text{key } k, \quad V_{\text{expected}}(k) = V_{\text{current}}(k) \implies \text{Commit Admitted}$$
$$V_{\text{expected}}(k) \ne V_{\text{current}}(k) \implies \text{Abort \& Automatic OCC Rollback}$$

Upon abort:
1. All tentative memory mutations are discarded.
2. Acquired multi-key locks are released.
3. Fencing tokens are invalidated.
4. `TX_ABORT` markers are appended to WAL for auditability.

---

## 5. Failure Recovery & Orphan Transactions

If the coordinator crashes during Phase 1 or Phase 2:
- On restart, `CrashRecoveryManager` scans WAL `TX_MARKER` entries.
- Uncommitted transactions past `timeout_seconds` are automatically aborted and locks unlocked.
- Committed transactions with missing state applications are replayed into the replicated state machine.
