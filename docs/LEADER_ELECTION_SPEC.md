# MeshWeaver Distributed Leader Election & Consensus Specification

## Overview
MeshWeaver's Distributed Consensus module provides randomized lease-based leader election, term validation, split-brain quorum safety, and proactive heartbeats across decentralized peer nodes.

---

## 1. Consensus State Machine

Each node exists in one of three mutually exclusive roles:

```
           [ Start / Timeout ]
                   │
                   ▼
            ┌──────────────┐
            │   FOLLOWER   │◄──────────────┐
            └──────┬───────┘               │
                   │ (Heartbeat Timeout)   │
                   ▼                       │
            ┌──────────────┐               │ (Higher Term
            │  CANDIDATE   │───────────────┤  Discovered)
            └──────┬───────┘               │
                   │ (Majority Quorum)     │
                   ▼                       │
            ┌──────────────┐               │
            │    LEADER    │───────────────┘
            └──────────────┘
```

### Role Transitions
1. **Follower $	o$ Candidate**: When no heartbeat lease is received within the randomized election timeout $T_{\text{election}} \in [T_{\min}, T_{\max}]$, follower transitions to candidate, increments `current_term`, and votes for itself.
2. **Candidate $	o$ Leader**: Upon securing a majority quorum of positive votes $V \ge \lfloor \frac{N}{2} \rfloor + 1$.
3. **Leader/Candidate $	o$ Follower**: Upon observing any RPC with term $T' > T_{\text{local}}$, immediately adopts $T'$ and steps down to follower.

---

## 2. Quorum & Timeout Mathematics

### Majority Quorum Formula
Given active gossip cluster size $N$:

$$Q = \left\lfloor \frac{N}{2} \right\rfloor + 1$$

- Single node ($N=1$): $Q = 1$ (immediate promotion)
- 3-node cluster ($N=3$): $Q = 2$ votes
- 5-node cluster ($N=5$): $Q = 3$ votes

### Randomized Election Timeout Jitter
To prevent split-vote deadlocks:

$$T_{\text{election}} = \text{Uniform}(T_{\min}, T_{\max})$$

Default values: $T_{\min} = 200\text{ms}$, $T_{\max} = 400\text{ms}$, Heartbeat interval $T_{\text{hb}} = 80\text{ms}$, Lease $T_{\text{lease}} = 500\text{ms}$.

---

## 3. Message RPC Specifications

- `ELECTION_VOTE_REQUEST`: `{term: int, candidate_id: str}`
- `ELECTION_VOTE_RESPONSE`: `{term: int, vote_granted: bool, voter_id: str}`
- `LEADER_HEARTBEAT`: `{term: int, leader_id: str, lease_duration: float, timestamp: float}`
- `LEADER_HEARTBEAT_ACK`: `{term: int, node_id: str, accepted: bool}`
