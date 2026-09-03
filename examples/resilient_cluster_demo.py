"""
MeshWeaver Example: Resilient Cluster Execution with Circuit Breakers.
Demonstrates automated fault isolation, circuit tripping (CLOSED -> OPEN),
safe failover routing, and recovery probing (OPEN -> HALF_OPEN -> CLOSED).
"""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.circuit_breaker import CircuitBreakerConfig
from meshweaver.node import MeshNode
from meshweaver.scheduler import RetryPolicy, SchedulingPolicy


def compute_heavy_task(item_id: int) -> str:
    """Worker task simulating data transformation."""
    return f"processed_item_{item_id}"


async def run_resilience_demo():
    print("=" * 65)
    print("   MeshWeaver Distributed Resilience & Circuit Breaker Demo")
    print("=" * 65)

    cb_cfg = CircuitBreakerConfig(
        failure_threshold=2,
        recovery_timeout=2.0,
        half_open_success_threshold=2,
    )

    # Initialize 3-node cluster
    print("\n[1] Starting 3-node Mesh Cluster (Coordinator + 2 Workers)...")
    coordinator = MeshNode(host="127.0.0.1", udp_port=10100, circuit_breaker_config=cb_cfg)
    worker_a = MeshNode(host="127.0.0.1", udp_port=10110, circuit_breaker_config=cb_cfg)
    worker_b = MeshNode(host="127.0.0.1", udp_port=10120, circuit_breaker_config=cb_cfg)

    await coordinator.start()
    await worker_a.start()
    await worker_b.start()

    # Interconnect nodes
    coordinator.register_neighbor(worker_a.node_id.hex(), "127.0.0.1", worker_a.bound_udp_port, worker_a.bound_tcp_port)
    coordinator.register_neighbor(worker_b.node_id.hex(), "127.0.0.1", worker_b.bound_udp_port, worker_b.bound_tcp_port)

    # Wait for gossip telemetry sync
    await asyncio.sleep(0.5)

    try:
        print("\n[2] Dispatching normal workload across healthy cluster...")
        for i in range(1, 4):
            res = await coordinator.schedule_task(compute_heavy_task, i, policy=SchedulingPolicy.ROUND_ROBIN)
            print(f"  -> Task {i} completed successfully: {res}")

        print("\n[3] Simulating sudden crash/failure of Worker A...")
        await worker_a.stop()
        worker_a_id = worker_a.node_id.hex()

        retry_policy = RetryPolicy(max_retries=2, backoff_factor=0.05)

        print("\n[4] Dispatching tasks with Worker A down (triggering circuit breaker)...")
        for i in range(4, 7):
            res = await coordinator.schedule_task(
                compute_heavy_task,
                i,
                policy=SchedulingPolicy.LEAST_LOADED,
                retry_policy=retry_policy,
            )
            status = coordinator.get_circuit_status(worker_a_id)
            print(
                f"  -> Task {i} result: {res} | Worker A Breaker: State={status['state']}, Failures={status['failure_count']}"
            )

        print(f"\n[5] Current Tripped Nodes: {coordinator.get_tripped_nodes()}")
        print("  -> Worker A circuit is now OPEN. Scheduler automatically excludes Worker A.")

        print("\n[6] Dispatching batch while Worker A is isolated (fast failover / bypass)...")
        start = time.perf_counter()
        batch_res, _ = await coordinator.map(
            compute_heavy_task,
            list(range(10, 16)),
            policy=SchedulingPolicy.LEAST_LOADED,
        )
        elapsed = time.perf_counter() - start
        print(f"  -> Batch of 6 items processed in {elapsed:.3f}s: {batch_res}")

        print(f"\n[7] Waiting {cb_cfg.recovery_timeout}s for recovery timeout to test HALF_OPEN state...")
        await asyncio.sleep(cb_cfg.recovery_timeout + 0.1)

        status_half_open = coordinator.get_circuit_status(worker_a_id)
        print(f"  -> Worker A Breaker State after timeout: {status_half_open['state']} (Probe Enabled)")

        print("\n[8] Restarting Worker A to simulate node recovery...")
        worker_a = MeshNode(
            host="127.0.0.1",
            udp_port=10110,
            tcp_port=worker_a.bound_tcp_port,
            node_id=worker_a.node_id,
            circuit_breaker_config=cb_cfg,
        )
        await worker_a.start()

        # Send probe tasks
        print("\n[9] Sending probe trial requests through HALF_OPEN circuit...")
        p1 = await coordinator.schedule_task(compute_heavy_task, 99)
        print(f"  -> Probe 1 succeeded: {p1}")
        p2 = await coordinator.schedule_task(compute_heavy_task, 100)
        print(f"  -> Probe 2 succeeded: {p2}")

        status_recovered = coordinator.get_circuit_status(worker_a_id)
        print(f"\n[10] Worker A Breaker Final State: {status_recovered['state']} (Healthy)")
        print(f"     Tripped nodes remaining: {coordinator.get_tripped_nodes()}")

        print("\n" + "=" * 65)
        print("   Circuit Breaker Resilience Demonstration Completed Successfully!")
        print("=" * 65)

    finally:
        await coordinator.stop()
        await worker_a.stop()
        await worker_b.stop()


if __name__ == "__main__":
    asyncio.run(run_resilience_demo())
