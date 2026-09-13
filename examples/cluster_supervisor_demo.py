"""
MeshWeaver Cluster Supervisor & Dynamic Node Orchestration Demo (Week 4 Day 5).
Demonstrates cluster runner supervision, dynamic node spawning, topology wiring,
simulated node crash recovery, and zero-downtime rolling cluster restarts.
"""

import asyncio
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.cluster_runner import ClusterConfig, LocalClusterRunner
from meshweaver.models import ClusterTopology, NodeLifecycleState, TxIsolationLevel


async def run_supervisor_demo():
    print("=" * 80)
    print("      MeshWeaver v1.0.0 - Cluster Supervisor & Dynamic Orchestration Demo")
    print("=" * 80)

    temp_dir = tempfile.mkdtemp(prefix="meshweaver_super_")
    runner = LocalClusterRunner(
        ClusterConfig(
            cluster_name="alpha_mesh_cluster",
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
        # 1. Start Supervised Cluster
        print("\n[Step 1] Initializing Supervised 3-Node Cluster...")
        nodes = await runner.start()
        await asyncio.sleep(0.3)

        # Trigger election
        await nodes[0].trigger_election()
        await asyncio.sleep(0.4)

        leader = runner.leader or nodes[0]
        print(f"  -> Cluster Running with 3 Nodes. Elected Leader: {leader.node_id.hex()[:8]}...")
        print("\nInitial Cluster Status Table:")
        print(runner.format_status_table())

        # Write state and 2PC transactions on node-2
        print("\n[Step 2] Executing Distributed 2PC Transactions on node-2...")
        node_2 = runner.get_node("node-2")
        async with await node_2.begin_transaction(isolation_level=TxIsolationLevel.SERIALIZABLE) as tx:
            tx.set("config:max_workers", 64)
            tx.set("config:cluster_mode", "HIGH_AVAILABILITY")
            tx.increment("system:epoch", delta=1)

        print(f"  -> State committed: config:max_workers = {node_2.state_get('config:max_workers')}")
        print(f"  -> State committed: config:cluster_mode = {node_2.state_get('config:cluster_mode')}")

        # 2. Sudden Node Crash Simulation
        print("\n[Step 3] Simulating Sudden Crash on 'node-2'...")
        ok = await runner.kill_node("node-2", simulated_crash=True)
        crashed_proc = runner.processes.get("node-2")
        print(f"  -> 'node-2' terminated abruptly. Process State: {crashed_proc.state.value}")

        print("\nCluster Status Table during Crash:")
        print(runner.format_status_table())

        # 3. Node Cold-Boot & WAL Replay Recovery
        print("\n[Step 4] Restoring and Cold-Booting 'node-2' from Disk WAL...")
        recovered_node = await runner.restart_node("node-2")
        print(f"  -> 'node-2' restored with NodeID: {recovered_node.node_id.hex()[:8]}...")
        print(f"  -> Recovered Data on node-2: config:max_workers = {recovered_node.state_get('config:max_workers')}")

        # 4. Zero-Downtime Rolling Cluster Reboot
        print("\n[Step 5] Executing Zero-Downtime Rolling Cluster Restart...")
        await runner.rolling_restart(delay_between_nodes=0.2)
        print("  -> Rolling reboot completed across all cluster nodes.")

        # Re-elect leader if needed
        await runner.trigger_cluster_election()
        await asyncio.sleep(0.3)

        # 5. Final Status & Summary
        print("\n[Step 6] Final Supervised Cluster Telemetry:")
        summary = runner.get_cluster_summary()
        print(f"  * Cluster Name:    {summary['cluster_name']}")
        print(f"  * Topology:        {summary['topology']}")
        print(f"  * Total Nodes:     {summary['total_nodes']}")
        print(f"  * Healthy Nodes:   {summary['healthy_nodes']}")
        print(f"  * Uptime Seconds:  {summary['uptime_seconds']:.2f}s")

        print("\nFinal Cluster Status Table:")
        print(runner.format_status_table())

    finally:
        print("\n[Teardown] Stopping cluster supervisor...")
        await runner.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("[Teardown] Supervisor demo completed successfully.")


if __name__ == "__main__":
    asyncio.run(run_supervisor_demo())
