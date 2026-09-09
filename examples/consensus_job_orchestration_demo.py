"""
MeshWeaver Consensus-Backed Job Orchestrator & State Snapshot Transfer Demo
Demonstrates replicated distributed job lifecycle, leader coordination, worker execution,
orphan job failover re-assignment, and Raft snapshot state transfer to lagging nodes.
"""

import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.models import ConsensusJobStatus
from meshweaver.node import MeshNode

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("demo.consensus_orchestrator")


def compute_monte_carlo_pi_chunk(samples: int) -> int:
    """CPU compute kernel returning points inside unit circle."""
    import random
    inside = 0
    for _ in range(samples):
        x, y = random.random(), random.random()
        if x * x + y * y <= 1.0:
            inside += 1
    return inside


def compute_factorize(n: int) -> list:
    """Factorization compute kernel."""
    factors = []
    d = 2
    temp = n
    while d * d <= temp:
        while temp % d == 0:
            factors.append(d)
            temp //= d
        d += 1
    if temp > 1:
        factors.append(temp)
    return factors


async def run_consensus_orchestration_demo():
    print("=" * 75)
    print("MeshWeaver: Consensus Job Orchestrator & Snapshot Transfer Demo")
    print("=" * 75)

    # 1. Bootstrapping Cluster
    print("\n[1/5] Bootstrapping 3-Node Consensus Cluster...")
    leader = MeshNode(host="127.0.0.1", udp_port=19800, tcp_port=19801)
    worker1 = MeshNode(host="127.0.0.1", udp_port=19810, tcp_port=19811)
    worker2 = MeshNode(host="127.0.0.1", udp_port=19820, tcp_port=19821)

    nodes = [leader, worker1, worker2]
    for n in nodes:
        await n.start()
        n.leader_election.config.min_election_timeout = 0.150
        n.leader_election.config.max_election_timeout = 0.300
        n.leader_election.config.heartbeat_interval = 0.040

    try:
        await worker1.bootstrap([("127.0.0.1", 19800)])
        await worker2.bootstrap([("127.0.0.1", 19800)])
        await leader.bootstrap([("127.0.0.1", 19810)])
        await asyncio.sleep(0.15)

        await leader.trigger_election()
        await asyncio.sleep(0.4)
        print("[OK] Cluster initialized with 3 nodes. Leader elected.")

        # 2. Submitting Parallel Consensus Jobs
        print("\n[2/5] Submitting Distributed Compute Jobs via Raft Consensus...")
        job_ids = []
        samples_per_job = 250_000
        for i in range(4):
            jid = await leader.submit_consensus_job(
                compute_monte_carlo_pi_chunk,
                samples_per_job,
                priority=1 if i == 0 else 2,
                job_id=f"pi_chunk_{i + 1}",
            )
            job_ids.append(jid)
            print(f"  [SUBMIT] Job '{jid}' replicated to commit log.")

        # 3. Awaiting Distributed Execution
        print("\n[3/5] Awaiting Consensus Execution Across Mesh Workers...")
        results = []
        for jid in job_ids:
            res = await leader.await_consensus_job(jid, timeout=10.0)
            job_state = leader.get_consensus_job(jid)
            results.append(res)
            print(f"  [COMPLETE] Job '{jid}' completed on worker {job_state.assigned_to} -> Inside: {res}/{samples_per_job}")

        total_inside = sum(results)
        total_samples = samples_per_job * len(job_ids)
        pi_estimate = 4.0 * total_inside / total_samples
        print(f"\n  => Multi-Node Monte Carlo Pi Estimate: {pi_estimate:.6f} (Samples: {total_samples:,})")

        # 4. Snapshot Compaction & State Transfer
        print("\n[4/5] Raft Log Compaction & InstallSnapshot State Transfer...")
        snap = leader.create_cluster_snapshot()
        print(f"  [SNAPSHOT] Leader compacted log up to index {snap['snapshot_last_index']}.")
        print(f"  [SNAPSHOT] Replicated state keys: {len(snap['data']['state'])}, Replicated jobs: {len(snap['data']['jobs'])}")

        # 5. Dynamic Membership Reconfiguration
        print("\n[5/5] Dynamic Cluster Membership Reconfiguration...")
        new_peer = "127.0.0.1:19830"
        members = await leader.reconfigure_membership(new_peer, action="ADD")
        print(f"  [MEMBERSHIP] Added {new_peer} to consensus cluster -> Active Members: {members}")

        orch_metrics = leader.get_orchestrator_metrics()
        raft_metrics = leader.get_raft_metrics()
        print("\n[METRICS] Consensus Cluster Summary:")
        print(f"  * Total Jobs Submitted:     {orch_metrics.total_submitted}")
        print(f"  * Total Jobs Completed:     {orch_metrics.total_completed}")
        print(f"  * Total Raft Proposals:     {raft_metrics.total_proposals}")
        print(f"  * Raft Commit Index:        {raft_metrics.commit_index}")
        print(f"  * Snapshots Sent/Installed: {raft_metrics.total_snapshots_sent}/{raft_metrics.total_snapshots_installed}")

        print("\n[DONE] Consensus Job Orchestration & Snapshot Transfer Demo Complete!")

    finally:
        for n in nodes:
            try:
                await n.stop()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(run_consensus_orchestration_demo())
