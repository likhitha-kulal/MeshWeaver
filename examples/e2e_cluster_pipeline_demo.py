"""
MeshWeaver End-to-End Enterprise Distributed Pipeline Demonstration (Week 4 Day 5).
Combines Kademlia DHT routing, Raft consensus state machines, 2PC ACID transactions,
priority batch scheduling, rendezvous synchronization barriers, and crash-resilient WAL recovery.
"""

import asyncio
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.cluster_runner import ClusterConfig, LocalClusterRunner
from meshweaver.models import (
    BarrierState,
    ClusterTopology,
    TxIsolationLevel,
)
from meshweaver.scheduler import SchedulingPolicy


def extract_features(text: str) -> dict:
    """CPU Worker Kernel: Extract word frequency features from text document."""
    words = [w.lower().strip(".,!?:;") for w in text.split()]
    freqs = {}
    for w in words:
        if len(w) > 2:
            freqs[w] = freqs.get(w, 0) + 1
    return {"token_count": len(words), "unique_terms": len(freqs), "frequencies": freqs}


async def run_e2e_pipeline_demo():
    print("=" * 80)
    print("  MeshWeaver v1.0.0 - End-to-End Distributed Compute & Consensus Pipeline")
    print("=" * 80)

    temp_dir = tempfile.mkdtemp(prefix="meshweaver_pipeline_")
    runner = LocalClusterRunner(
        ClusterConfig(
            cluster_name="enterprise_pipeline_mesh",
            node_count=3,
            base_udp_port=0,
            base_tcp_port=0,
            topology=ClusterTopology.FULL_MESH,
            data_dir=temp_dir,
            auto_bootstrap=True,
            enable_wal=True,
        )
    )

    try:
        # 1. Cluster Initialization & Leader Election
        print("\n[Stage 1] Initializing 3-Node Enterprise Supervised Mesh...")
        nodes = await runner.start()
        await asyncio.sleep(0.3)

        await nodes[0].trigger_election()
        await asyncio.sleep(0.4)

        leader = runner.leader or nodes[0]
        print(f"  -> Cluster Online! Active Leader: {leader.node_id.hex()[:8]}...")

        # 2. Replicated Consensus Configuration
        print("\n[Stage 2] Setting Up Replicated State Machine Configurations...")
        await leader.state_set("pipeline:name", "NLP_Ingestion_Engine")
        await leader.state_set("pipeline:stage", "INITIALIZING")
        await leader.state_increment("pipeline:run_counter", delta=1)

        print(f"  -> Pipeline: {leader.state_get('pipeline:name')} (Run #{leader.state_get('pipeline:run_counter')})")

        # 3. 2PC Financial / Compute Budget Transaction
        print("\n[Stage 3] Executing 2-Phase Commit (2PC) Compute Resource Allocation...")
        async with await leader.begin_transaction(isolation_level=TxIsolationLevel.SERIALIZABLE) as tx:
            tx.set("ledger:project_budget", 50000)
            tx.set("ledger:allocated_units", 120)
            tx.increment("ledger:allocations_count", delta=1)

        print(f"  -> 2PC Resource Allocation Committed: Budget={leader.state_get('ledger:project_budget')} units")

        # 4. Distributed Batch Compute Execution
        print("\n[Stage 4] Distributing Parallel Feature Extraction Workload...")
        documents = [
            "MeshWeaver is an autonomous peer to peer distributed compute mesh.",
            "Consensus engines synchronize state machines reliably with Raft.",
            "Write Ahead Logs ensure absolute crash resilience and ACID recovery.",
            "Dynamic load shedding and circuit breakers prevent cascade failures.",
            "Distributed barriers rendezvous compute stages across worker nodes.",
            "Two Phase Commit transactions provide atomic multi key consistency.",
        ]

        t0 = time.perf_counter()
        results, b_metrics = await leader.map(
            extract_features,
            documents,
            policy=SchedulingPolicy.LEAST_LOADED,
        )
        elapsed = time.perf_counter() - t0

        print(f"  -> Completed {len(results)} Documents in {elapsed:.3f}s (Throughput: {b_metrics.throughput:.1f} items/s)")
        total_tokens = sum(r["token_count"] for r in results)
        total_unique = sum(r["unique_terms"] for r in results)
        print(f"  -> Extracted {total_tokens} Total Tokens ({total_unique} Unique Terms across batch)")

        # 5. Distributed Synchronization Barrier Rendezvous
        print("\n[Stage 5] Synchronizing Multi-Node Rendezvous Barrier...")
        barrier_id = "stage_1_completion_barrier"
        barrier = leader.create_barrier(barrier_id, threshold=3, timeout_seconds=5.0)

        async def worker_rendezvous(node, wid):
            return await barrier.enter(wid, timeout=4.0)

        b_tasks = [
            asyncio.create_task(worker_rendezvous(nodes[0], "worker_0")),
            asyncio.create_task(worker_rendezvous(nodes[1], "worker_1")),
            asyncio.create_task(worker_rendezvous(nodes[2], "worker_2")),
        ]
        b_res = await asyncio.gather(*b_tasks)
        print(f"  -> Rendezvous Barrier Reached by 3 Workers: Status={barrier.state.name} (Quorum: {all(b_res)})")

        # 6. Sudden Crash & Cold-Boot Recovery
        print("\n[Stage 6] Simulating Abrupt Leader Node Failure & Cold-Boot Recovery...")
        leader_node_name = next(name for name, n in runner.nodes.items() if n == leader)
        await runner.kill_node(leader_node_name, simulated_crash=True)
        print(f"  -> '{leader_node_name}' crashed abruptly!")

        # Restore from WAL
        rebooted_leader = await runner.restart_node(leader_node_name)
        print(f"  -> '{leader_node_name}' restored and replayed WAL records from disk!")
        print(f"  -> Replayed Transaction Data: Budget = {rebooted_leader.state_get('ledger:project_budget')}")
        print(f"  -> Replayed Transaction Data: Units  = {rebooted_leader.state_get('ledger:allocated_units')}")

        # 7. Final Pipeline Status Table
        print("\n[Stage 7] Enterprise Supervised Mesh Health Status:")
        print(runner.format_status_table())

    finally:
        print("\n[Teardown] Shutting down enterprise pipeline mesh...")
        await runner.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("[Teardown] Pipeline demonstration completed successfully.")


if __name__ == "__main__":
    asyncio.run(run_e2e_pipeline_demo())
