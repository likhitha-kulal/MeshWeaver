"""
MeshWeaver Priority QoS & Starvation-Free Aging Demonstration.
Simulates a multi-node compute mesh executing mixed workloads:
heavy background batch crunching vs. urgent interactive and critical queries.
"""

import asyncio
import os
import sys
import time

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from meshweaver.node import MeshNode
from meshweaver.priority_queue import TaskPriority


def compute_simulation_workload(label: str, duration_ms: int = 100) -> str:
    """Simulate CPU compute workload."""
    start = time.perf_counter()
    # Busy loop work
    total = 0
    for i in range(duration_ms * 1000):
        total += (i % 7)
    elapsed = (time.perf_counter() - start) * 1000.0
    return f"Done: {label} (work_val={total}, took {elapsed:.1f}ms)"


def render_ascii_qos_table(metrics: dict, title: str = "MESHWEAVER PRIORITY QoS MONITOR"):
    """Render a terminal table for QoS metrics."""
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)
    print(f"  Total Enqueued : {metrics.get('total_enqueued', 0):<8} | Completed : {metrics.get('total_completed', 0):<8}")
    print(f"  Failed Tasks   : {metrics.get('total_failed', 0):<8} | Cancelled : {metrics.get('total_cancelled', 0):<8}")
    print(f"  Aged Promotions: {metrics.get('total_aged_promotions', 0):<8} | Avg Wait  : {metrics.get('avg_wait_time_ms', 0):.2f} ms")
    print("-" * 65)
    print("  QoS Tiers Breakdown:")
    tier_counts = metrics.get("tasks_by_priority", {})
    for tier, count in tier_counts.items():
        bar = "█" * (count * 2)
        print(f"    [{tier:<10}] : {count:>3} tasks  {bar}")
    print("=" * 65 + "\n")


async def run_qos_demonstration():
    print("=" * 65)
    print("  🚀 Starting MeshWeaver Multi-Node Priority QoS Cluster...")
    print("=" * 65)

    # 1. Start coordinator and 3 compute workers
    coordinator = MeshNode(host="127.0.0.1", udp_port=21000)
    worker1 = MeshNode(host="127.0.0.1", udp_port=21002)
    worker2 = MeshNode(host="127.0.0.1", udp_port=21004)
    worker3 = MeshNode(host="127.0.0.1", udp_port=21006)

    nodes = [coordinator, worker1, worker2, worker3]
    for n in nodes:
        await n.start()

    # Wire gossip
    for w in [worker1, worker2, worker3]:
        coordinator.register_neighbor(w.node_id.hex(), "127.0.0.1", w.bound_udp_port, w.bound_tcp_port)
        w.register_neighbor(coordinator.node_id.hex(), "127.0.0.1", coordinator.bound_udp_port, coordinator.bound_tcp_port)

    # Enable QoS dispatcher on coordinator with concurrency limit of 3
    coordinator.scheduler.enable_priority_queue(concurrency=3, aging_interval_seconds=1.5)
    await coordinator.scheduler.priority_dispatcher.start()

    print("✅ Cluster running with 1 Coordinator & 3 Workers. Waiting for gossip convergence...")
    await asyncio.sleep(0.5)

    print("\n📦 Enqueuing 12 Low-Priority / Background Batch Tasks...")
    batch_futs = []
    for i in range(1, 13):
        f = await coordinator.submit_prioritized(
            compute_simulation_workload,
            f"Batch_ETL_Chunk_{i}",
            60,
            priority=TaskPriority.LOW if i % 2 == 0 else TaskPriority.BACKGROUND,
            task_id=f"batch_job_{i}",
        )
        batch_futs.append((f"Batch_{i}", f))

    print(f"📊 Queued {len(batch_futs)} batch jobs.")

    # Small delay
    await asyncio.sleep(0.05)

    print("\n⚡ Injecting Urgent High-Priority & Critical User Queries!")
    crit_start = time.perf_counter()
    crit_fut = await coordinator.submit_prioritized(
        compute_simulation_workload,
        "EMERGENCY_HEALTH_PROBE",
        20,
        priority=TaskPriority.CRITICAL,
        task_id="vip_critical_01",
    )

    high_fut = await coordinator.submit_prioritized(
        compute_simulation_workload,
        "INTERACTIVE_USER_QUERY",
        30,
        priority=TaskPriority.HIGH,
        task_id="user_query_01",
    )

    # Await VIP results
    res_crit = await crit_fut
    crit_duration = (time.perf_counter() - crit_start) * 1000.0
    print(f"  ⭐ [CRITICAL FINISHED] in {crit_duration:.2f}ms -> {res_crit}")

    res_high = await high_fut
    print(f"  ✨ [HIGH FINISHED] -> {res_high}")

    # Render intermediate snapshot
    render_ascii_qos_table(coordinator.get_queue_metrics(), "LIVE QoS SNAPSHOT (POST VIP INJECTION)")

    print("⏳ Awaiting remainder of background batch workloads...")
    for label, f in batch_futs:
        res = await f
        print(f"  -> {label} completed.")

    # Final QoS summary
    render_ascii_qos_table(coordinator.get_queue_metrics(), "FINAL COMPLETED QoS SUMMARY")

    # Cleanup
    print("🛑 Shutting down cluster nodes...")
    for n in nodes:
        await n.stop()
    print("✅ QoS Demonstration completed successfully.")


if __name__ == "__main__":
    asyncio.run(run_qos_demonstration())
