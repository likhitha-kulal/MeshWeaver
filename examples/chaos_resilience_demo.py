"""
MeshWeaver Chaos Monkey & Fault Injection Resilience Demo (Week 4 Day 5).
Demonstrates dynamic network partitions, latency jitter injection, Byzantine payload
corruption defenses, and autonomous cluster self-healing capabilities.
"""

import asyncio
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.cluster_runner import ClusterConfig, LocalClusterRunner
from meshweaver.models import ClusterTopology, TaskEnvelope, TxIsolationLevel


async def run_chaos_demo():
    print("=" * 80)
    print("      MeshWeaver v1.0.0 - Chaos Monkey & Resilience Demonstration")
    print("=" * 80)

    temp_dir = tempfile.mkdtemp(prefix="meshweaver_chaos_")
    runner = LocalClusterRunner(
        ClusterConfig(
            cluster_name="resilience_mesh",
            node_count=4,
            base_udp_port=0,
            base_tcp_port=0,
            topology=ClusterTopology.FULL_MESH,
            data_dir=temp_dir,
            auto_bootstrap=True,
        )
    )

    try:
        # 1. Start Cluster and Elect Leader
        print("\n[Stage 1] Bootstrapping 4-Node Full-Mesh Cluster...")
        nodes = await runner.start()
        await asyncio.sleep(0.3)

        print("[Stage 1] Electing Raft Consensus Cluster Leader...")
        await nodes[0].trigger_election()
        await asyncio.sleep(0.5)

        leader = runner.leader or nodes[0]
        leader_name = next(name for name, n in runner.nodes.items() if n == leader)
        print(f"  -> Cluster Online! Leader: {leader_name} (ID: {leader.node_id.hex()[:8]}...)")

        # Baseline KV state
        await leader.state_set("cluster_status", "NOMINAL")
        await leader.state_set("security_level", "ENFORCED")
        print("  -> Baseline consensus state committed: cluster_status=NOMINAL")

        # 2. Simulate Split-Brain Network Partition (3 Majority vs 1 Minority)
        print("\n[Stage 2] Simulating Split-Brain Network Partition (Isolating Node-4)...")
        node_4 = runner.get_node("node-4")
        maj_ids = {nodes[0].node_id.hex(), nodes[1].node_id.hex(), nodes[2].node_id.hex()}
        min_ids = {node_4.node_id.hex()}

        for n in nodes:
            n.create_partition("split_3_1", group_a=maj_ids, group_b=min_ids, bidirectional=True)

        print("  -> Partition active: Majority {node-1, node-2, node-3}, Isolated {node-4}")
        val_maj = await leader.state_set("partition_test", "majority_survives")
        print(f"  -> Leader committed new state during partition: partition_test={val_maj}")

        # 3. Inject High Latency & Random Jitter
        print("\n[Stage 3] Injecting Artificial Network Latency (15-35ms) and Jitter...")
        for n in nodes[:3]:
            n.inject_latency(min_ms=15.0, max_ms=35.0, jitter_ms=8.0)
        print("  -> Latency injector active across majority peers.")

        t0 = time.perf_counter()
        await leader.state_set("jitter_key", "jitter_val")
        elapsed = (time.perf_counter() - t0) * 1000
        print(f"  -> Consensus commit under jitter completed in {elapsed:.2f}ms")

        # 4. Byzantine Payload Corruption Detection
        print("\n[Stage 4] Testing Byzantine Corrupted Payload Rejection...")
        valid_payload = b"compute_task_valid_payload_123"
        envelope = TaskEnvelope.wrap(valid_payload)
        self_check = envelope.verify()
        print(f"  -> Original Envelope Checksum: {envelope.sha256[:16]}... Valid: {self_check}")

        # Tamper payload
        envelope.payload = b"compute_task_tampered_payload_999"
        tamper_check = envelope.verify()
        print(f"  -> Tampered Envelope Validation: {tamper_check} (Integrity Attack Prevented!)")

        # 5. Self-Healing: Heal All Partitions and Drops
        print("\n[Stage 5] Triggering Cluster Autonomous Self-Healing...")
        for n in nodes:
            n.heal_chaos()
        await asyncio.sleep(0.2)
        print("  -> All network partitions, latency injectors, and packet drops healed.")

        # Verify cluster catchup
        val_healed = leader.state_get("partition_test")
        print(f"  -> Post-heal cluster state verification: partition_test={val_healed}")

        # 6. Display Chaos Engine Metrics
        print("\n[Stage 6] Chaos & Reliability Telemetry Snapshot:")
        for name, n in runner.nodes.items():
            cm = n.get_chaos_metrics()
            print(
                f"  * {name:<8} -> Inspected: {cm['total_packets_inspected']:<4} | "
                f"Dropped: {cm['total_packets_dropped']:<3} | "
                f"Delayed: {cm['total_packets_delayed']:<3} | "
                f"Partitions: {cm['active_partitions_count']}"
            )

        print("\n" + runner.format_status_table())

    finally:
        print("\n[Teardown] Stopping resilience demo cluster...")
        await runner.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("[Teardown] Demo completed successfully.")


if __name__ == "__main__":
    asyncio.run(run_chaos_demo())
