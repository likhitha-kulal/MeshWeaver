# MeshWeaver Priority QoS & Starvation-Free Aging Specification

## 1. Overview & Motivation

In a decentralized peer-to-peer compute mesh, diverse workloads run concurrently:
- Low-latency interactive queries (e.g., node health probes, user RPCs)
- Medium-priority ad-hoc functions
- Long-running batch pipelines, ETL jobs, and speculative background tasks

Without a Quality of Service (QoS) tiering and priority queueing system, heavy batch workloads saturate worker nodes and cause unbounded queuing latency for critical control and user tasks.

MeshWeaver's **Priority Task Queue & QoS Engine** (`meshweaver.priority_queue`) provides multi-tier priority dispatching, preemption precedence, and mathematical starvation prevention through dynamic aging.

---

## 2. QoS Priority Tiers

| Priority Tier | Level ($P_{\text{base}}$) | Target SLA / Purpose | Example Operations |
| :--- | :--- | :--- | :--- |
| **`CRITICAL`** | `0` | Immediate dispatch (< 50ms) | Cluster health probes, circuit breaker resets, cluster coordination |
| **`HIGH`** | `1` | Low latency (< 200ms) | Interactive user requests, real-time analytics queries |
| **`NORMAL`** | `2` | Default standard SLA | Ad-hoc user functions, distributed map steps |
| **`LOW`** | `3` | Best effort batch processing | Multi-stage ETL pipelines, large batch maps |
| **`BACKGROUND`** | `4` | Idle cycle execution | Speculative compute, DHT cache pre-warming, routine indexing |

---

## 3. Mathematical Formulas

### 3.1 Dynamic Effective Priority Formula

To prevent high-priority starvation of low-priority tasks while guaranteeing preemption for urgent requests, task precedence in the min-heap is governed by **Effective Priority ($P_{\text{eff}}$)**:

$$P_{\text{eff}}(t) = P_{\text{base}} - \Delta_{\text{age}}(t) - \Delta_{\text{deadline}}(t)$$

Where lower numerical score indicates higher dispatch precedence ($P_{\text{eff}} \le 0$ achieves or exceeds `CRITICAL` precedence).

---

### 3.2 Starvation Prevention via Time-Based Aging

Tasks waiting in the queue accrue priority promotion proportional to elapsed wait duration:

$$\Delta_{\text{age}}(t) = \frac{t - t_{\text{created}}}{T_{\text{aging}}}$$

- $t - t_{\text{created}}$: Elapsed wait duration in seconds.
- $T_{\text{aging}}$: Configurable aging interval (default: $2.0\,\text{s}$).

**Starvation-Free Theorem:**
For any task $T_i$ with base priority $P_{\text{base}} = 4$ (`BACKGROUND`) and aging interval $T_{\text{aging}} = 2.0\,\text{s}$, after waiting $8.0\,\text{s}$:
$$\Delta_{\text{age}} = \frac{8.0}{2.0} = 4.0 \implies P_{\text{eff}} = 4.0 - 4.0 = 0.0$$
The background task ascends to `CRITICAL` priority tier, preventing indefinite starvation regardless of incoming high-priority traffic.

---

### 3.3 Deadline-Aware Urgency Promotion

When a task specifies an execution deadline $t_{\text{deadline}}$:

$$\Delta_{\text{deadline}}(t) = \begin{cases} 
2.0 \times W_{\text{deadline}}, & \text{if } t \ge t_{\text{deadline}} \text{ (overdue)} \\
W_{\text{deadline}} \times \left(\frac{T_{\text{threshold}} - (t_{\text{deadline}} - t)}{T_{\text{threshold}}}\right), & \text{if } 0 < t_{\text{deadline}} - t < T_{\text{threshold}} \\
0.0, & \text{otherwise}
\end{cases}$$

- $W_{\text{deadline}}$: Urgency boost weight (default: $2.0$).
- $T_{\text{threshold}}$: Critical window threshold (default: $5.0\,\text{s}$).

---

### 3.4 FIFO Tie-Breaking

When two tasks evaluate to identical effective priorities ($|P_{\text{eff}, A} - P_{\text{eff}, B}| \le 10^{-4}$):

$$\text{Precedence}(A, B) = \text{seq}(A) < \text{seq}(B)$$

Strict FIFO ordering is preserved for identical-priority jobs using monotonic sequence integers.

---

## 4. Architectural State Flow

```
[ Client / MeshNode ]
         │
         │  submit_prioritized(fn, priority=HIGH)
         ▼
[ PriorityTaskQueue ] ──(Min-Heap with Dynamic Re-heapify)
         │
         ├─ calculate_effective_priority(aging, deadline)
         ├─ heapify on Pop/Peek
         │
         ▼
[ PriorityDispatcher (Worker Pool) ]
         │
         ▼
[ TaskScheduler (LoadScorer + CircuitBreaker) ]
         │
         ▼
[ TCP Remote Execution / Local Fallback ]
```

---

## 5. Telemetry & QoS Metrics (`PriorityMetrics`)

- `total_enqueued`: Total count of tasks submitted across all priority levels.
- `total_completed`: Successfully executed prioritized tasks.
- `total_failed`: Tasks that encountered unhandled remote execution errors.
- `total_cancelled`: Tasks cancelled before dispatch.
- `total_aged_promotions`: Count of priority promotions triggered by starvation aging.
- `avg_wait_time_ms`: Rolling average queue waiting latency in milliseconds.
- `tasks_by_priority`: Active breakdown across `CRITICAL`, `HIGH`, `NORMAL`, `LOW`, `BACKGROUND`.

---

## 6. Python Usage Example

```python
from meshweaver import MeshNode, TaskPriority

node = MeshNode(host="127.0.0.1", udp_port=9000)
await node.start()

# Submit background batch computation
bg_fut = await node.submit_prioritized(
    heavy_etl_job, 
    dataset, 
    priority=TaskPriority.BACKGROUND
)

# Submit urgent user query
urgent_fut = await node.submit_prioritized(
    fetch_user_profile, 
    user_id, 
    priority=TaskPriority.CRITICAL, 
    deadline=time.time() + 2.0
)

# Urgent task executes with immediate preemption
urgent_result = await urgent_fut
bg_result = await bg_fut
```
