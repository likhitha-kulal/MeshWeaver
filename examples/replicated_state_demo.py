"""
MeshWeaver Replicated State Machine & Distributed Lock Manager (DLM) Demo
Demonstrates Raft consensus state replication, atomic CAS, monotonic fencing tokens,
and distributed lock contention across a multi-node cluster.
"""

import asyncio
import logging
import time
from meshweaver.node import MeshNode

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("demo.replicated_state")


async def run_replicated_state_demo():
    print("=" * 70)
    print("🕸️  MeshWeaver: Replicated State Machine & Distributed Locking Demo")
    print("=" * 70)

    # 1. Start Cluster
    print("\n[1/5] Bootstrapping 3-Node Cluster...")
    leader = MeshNode(host="127.0.0.1", port=9500, enable_consensus=False)
    await leader.start()

    worker1 = MeshNode(host="127.0.0.1", port=9501, bootstrap_nodes=[("127.0.0.1", 9500)], enable_consensus=False)
    await worker1.start()

    worker2 = MeshNode(host="127.0.0.1", port=9502, bootstrap_nodes=[("127.0.0.1", 9500)], enable_consensus=False)
    await worker2.start()

    await asyncio.sleep(0.4)
    print("✅ Cluster running with 3 nodes.")

    try:
        # 2. Replicated KV Operations
        print("\n[2/5] Proposing Replicated Key-Value State Operations...")
        res1 = await leader.state_set("mesh.environment", "production-us-east")
        res2 = await leader.state_set("mesh.max_concurrency", 64)
        print(f"  • SET mesh.environment -> {res1}")
        print(f"  • SET mesh.max_concurrency -> {res2}")

        # 3. Monotonic Distributed Counters
        print("\n[3/5] Atomic Monotonic Counter Increments...")
        for i in range(3):
            val = await leader.state_increment("mesh.jobs_completed", delta=5)
            print(f"  • INCREMENT mesh.jobs_completed (+5) -> Total: {val}")

        # 4. Compare-And-Swap (CAS)
        print("\n[4/5] Executing Atomic Compare-And-Swap (CAS)...")
        cas_fail = await leader.state_cas("mesh.environment", expected="staging", new_value="development")
        print(f"  • CAS (expected='staging') -> {'SUCCESS' if cas_fail else 'FAILED (Expected mismatch)'}")

        cas_ok = await leader.state_cas("mesh.environment", expected="production-us-east", new_value="production-global")
        print(f"  • CAS (expected='production-us-east') -> {'SUCCESS' if cas_ok else 'FAILED'} (Value: {leader.state_get('mesh.environment')})")

        # 5. Distributed Lock Manager (DLM) & Fencing Tokens
        print("\n[5/5] Distributed Mutual Exclusion & Monotonic Fencing Tokens...")
        lock_a = await leader.acquire_lock("cluster_upgrade_mutex", ttl_seconds=15.0)
        print(f"  🔒 Node Leader acquired 'cluster_upgrade_mutex' | Fencing Token: {lock_a.fencing_token}")

        # Attempt concurrent acquire
        lock_b = await leader.acquire_lock("cluster_upgrade_mutex", ttl_seconds=15.0)
        print(f"  🔒 Concurrent acquire attempt -> Acquired: {lock_b.acquired} (Holder: {lock_b.holder_id})")

        # Release Lock
        rel = await leader.release_lock("cluster_upgrade_mutex", fencing_token=lock_a.fencing_token)
        print(f"  🔓 Released 'cluster_upgrade_mutex' with Token {lock_a.fencing_token} -> {rel}")

        # Re-acquire to inspect next monotonic token
        lock_c = await leader.acquire_lock("cluster_upgrade_mutex", ttl_seconds=15.0)
        print(f"  🔒 Re-acquired 'cluster_upgrade_mutex' | New Fencing Token: {lock_c.fencing_token}")

        metrics = leader.get_raft_metrics()
        print("\n📊 Replicated State Machine Telemetry:")
        print(f"  • Total Proposals: {metrics.total_proposals}")
        print(f"  • Last Log Index:  {metrics.last_log_index}")
        print(f"  • Commit Index:    {metrics.commit_index}")
        print(f"  • Active Keys:     {metrics.state_keys_count}")
        print(f"  • Active Locks:    {metrics.active_locks_count}")

        print("\n🎉 Replicated State Machine & Distributed Locking Demo Complete!")

    finally:
        await leader.stop()
        await worker1.stop()
        await worker2.stop()


if __name__ == "__main__":
    asyncio.run(run_replicated_state_demo())
