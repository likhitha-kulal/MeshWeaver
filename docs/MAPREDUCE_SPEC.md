# MeshWeaver MapReduce & Pipeline Architecture Specification

## 📌 Overview

MeshWeaver provides robust distributed data processing primitives:
1. **`DistributedMapReduce`**: Full distributed Map $\to$ Shuffle/Partition $\to$ Reduce compute engine.
2. **`tree_reduce`**: Hierarchical $O(\log_b N)$ parallel tree reduction for associative operators.
3. **`TaskPipeline` / DAG Engine**: Multi-stage chained workflow computation.

---

## 🏗️ 1. MapReduce Execution Pipeline

```
+------------------------------------------------------------------------+
|                          Input Dataset (Items)                         |
+------------------------------------------------------------------------+
                                    |
                                    v (Chunking & Concurrency Throttle)
                     [ Distributed Map Phase ]
        +-----------------------+-----------------------+
        |                       |                       |
        v                       v                       v
 [ Worker A (Map) ]      [ Worker B (Map) ]      [ Worker C (Map) ]
        |                       |                       |
        v                       v                       v
 [(k1,v1), (k2,v1)]      [(k1,v2), (k3,v1)]      [(k2,v2), (k1,v3)]
        +-----------------------+-----------------------+
                                    |
                                    v (Local Grouping / Partitioning)
                   [ Shuffle & Partition Phase ]
        Group: k1 -> [v1, v2, v3]
        Group: k2 -> [v1, v2]
        Group: k3 -> [v1]
                                    |
                                    v
                    [ Distributed Reduce Phase ]
        +-----------------------+-----------------------+
        |                       |                       |
        v                       v                       v
 [ Worker A (Reduce) ]   [ Worker B (Reduce) ]   [ Worker C (Reduce) ]
   k1 -> ReducedVal1       k2 -> ReducedVal2       k3 -> ReducedVal3
        +-----------------------+-----------------------+
                                    |
                                    v
                       Final Output: {k1: R1, k2: R2, k3: R3}
```

---

## 🌳 2. Tree-Based Hierarchical Reduction (`tree_reduce`)

For associative operations (like sum, min, max, product, matrix merge):
$$\text{Level } 0: [x_1, x_2, x_3, x_4, x_5, x_6, x_7, x_8]$$
$$\text{Level } 1: [f(x_1, x_2), f(x_3, x_4), f(x_5, x_6), f(x_7, x_8)]$$
$$\text{Level } 2: [f(y_1, y_2), f(y_3, y_4)]$$
$$\text{Level } 3: f(z_1, z_2) = \text{Final Result}$$

---

## ⛓️ 3. TaskPipeline Workflow Engine

```python
pipeline = node.create_pipeline()
pipeline.pipe("Stage 1: Extract/Parse", parse_fn, is_parallel=True)
pipeline.pipe("Stage 2: Normalize", normalize_fn, is_parallel=True)
pipeline.add_stage("Stage 3: Aggregate", aggregate_fn, is_parallel=False)

output, metrics = await pipeline.execute(dataset)
```
