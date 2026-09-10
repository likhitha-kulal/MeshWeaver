"""
MeshWeaver Distributed Synchronization & Dynamic Backpressure Demo.

Demonstrates:
1. DistributedBarrier rendezvous across multi-stage parallel pipelines.
2. DistributedCountdownLatch fan-out completion gating.
3. DistributedSemaphore resource concurrency pooling with lease TTL protection.
4. AdaptiveLoadShedder token-bucket throttling and QoS admission control.
5. Live ASCII Telemetry visualization.
"""

import asyncio
import os
import shutil
import sys
import tempfile
import time

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.adaptive_load_shedder import AdaptiveLoadShedder
from meshweaver.dashboard import ClusterTelemetryDashboard
from meshweaver.models import (
    BackpressureStatus,
    DistributedBarrierSpec,
    DistributedSemaphoreSpec,
    StorageConfig,
    TokenBucketConfig,
)
from meshweaver.node import MeshNode


async def run_demo():
    print("=" * 80)
    print("MESHWEAVER DISTRIBUTED SYNCHRONIZATION & BACKPRESSURE DEMO")
    print("=" * 80)

    temp_dir = tempfile.mkdtemp(prefix="meshweaver_sync_demo_")
    nodes = []

    try:
        # Step 1: Initialize 3-node cluster
        print("\n[Step 1] Bootstrapping 3-Node Compute Mesh...")
        for i in range(3):
            storage_cfg = StorageConfig(
                data_dir=temp_dir,
                node_storage_id=f"sync_node_{i}",
            )
            node = MeshNode(
                host="127.0.0.1",
                udp_port=9840 + i * 2,
                tcp_port=9841 + i * 2,
                storage_config=storage_cfg,
            )
            nodes.append(node)
            await node.start()
            print(f"  [+] Node {i+1} online: ID={node.node_id.hex()[:8]} (UDP={node.bound_udp_port})")

        # Step 2: Distributed Barrier Pipeline Rendezvous
        print("\n[Step 2] Executing 3-Worker Multi-Stage Barrier Rendezvous...")
        primary = nodes[0]
        barrier = primary.synchronization_manager.get_or_create_barrier("pipeline_stage_1", threshold=3)

        async def worker_pipeline(worker_id: str, delay: float):
            print(f"  --> Worker {worker_id} computing Stage 1 (Extract & Transform, {delay}s)...")
            await asyncio.sleep(delay)
            print(f"  --> Worker {worker_id} reached Barrier 'pipeline_stage_1'. Awaiting peers...")
            released = await barrier.enter(worker_id)
            print(f"  <-- Worker {worker_id} passed Barrier! (released={released})")

        await asyncio.gather(
            worker_pipeline("worker_1", 0.05),
            worker_pipeline("worker_2", 0.12),
            worker_pipeline("worker_3", 0.08),
        )
        print("  [+] All workers synchronized across pipeline barrier!")

        # Step 3: Distributed CountdownLatch Fan-Out
        print("\n[Step 3] Executing Distributed Countdown Latch (Fan-Out 4 Partitions)...")
        latch = primary.synchronization_manager.get_or_create_latch("batch_latch", count=4)

        async def partition_job(part_id: int):
            await asyncio.sleep(0.02 * part_id)
            remaining = await latch.count_down()
            print(f"  [+] Partition {part_id} completed. Remaining in latch: {remaining}")

        async def coordinator_wait():
            print("  --> Coordinator waiting on batch_latch...")
            await latch.wait()
            print("  <-- Coordinator released! All 4 partitions completed.")

        await asyncio.gather(
            coordinator_wait(),
            partition_job(1),
            partition_job(2),
            partition_job(3),
            partition_job(4),
        )

        # Step 4: Distributed Semaphore Concurrency Limiting
        print("\n[Step 4] Executing Distributed Counting Semaphore (Max 2 Leased Slots)...")
        sem = primary.synchronization_manager.get_or_create_semaphore("gpu_pool", total_permits=2, default_ttl_seconds=5.0)

        async def gpu_task(task_id: str):
            acquired = await sem.acquire(task_id, ttl_seconds=2.0)
            print(f"  [+] Task {task_id} acquired GPU slot: {acquired} (available permits: {sem.available_permits})")
            await asyncio.sleep(0.08)
            released = await sem.release(task_id)
            print(f"  [-] Task {task_id} released GPU slot: {released} (available permits: {sem.available_permits})")

        await asyncio.gather(
            gpu_task("job_A"),
            gpu_task("job_B"),
            gpu_task("job_C"),
        )

        # Step 5: Adaptive Load Shedding & QoS Watermark Simulation
        print("\n[Step 5] Simulating Adaptive Load Shedder & Backpressure QoS Admission...")
        shedder = AdaptiveLoadShedder(
            config=TokenBucketConfig(capacity=10.0, refill_rate=5.0, min_refill_rate=1.0)
        )

        # Under low load
        shedder.update_resource_telemetry(cpu_percent=20.0, ram_percent=30.0)
        w1 = shedder.calculate_watermark()
        st1 = shedder.get_status()
        admitted1 = await shedder.try_acquire(priority=2)
        print(f"  [+] Normal Load: Watermark W={w1:.3f} | Status={st1.value} | Admitted={admitted1}")

        # Under extreme load spike
        shedder.update_resource_telemetry(cpu_percent=95.0, ram_percent=90.0)
        w2 = shedder.calculate_watermark()
        st2 = shedder.get_status()
        # High priority admitted
        admitted_high = await shedder.try_acquire(priority=0)
        # Background priority shed
        admitted_bg = await shedder.try_acquire(priority=4)
        print(f"  [!] High Load Spike: Watermark W={w2:.3f} | Status={st2.value}")
        print(f"      - Critical/High Priority Task Admitted: {admitted_high}")
        print(f"      - Background/Low Priority Task Admitted: {admitted_bg} (Shed under load protection)")

        # Step 6: Render Telemetry Dashboard Frame
        dashboard = ClusterTelemetryDashboard(use_ansi_color=False)
        print("\n[Step 6] Cluster Telemetry Snapshot:")
        print(dashboard.render_frame(nodes))

    finally:
        print("\n[Step 7] Tearing down cluster...")
        for node in nodes:
            await node.stop()
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("  [+] All nodes stopped and test artifacts cleared.")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(run_demo())
