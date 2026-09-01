"""
MeshWeaver Example: Distributed Monte Carlo Pi Estimation
Demonstrates parallel random sampling batches and hierarchical tree reduction across mesh workers.
"""

import asyncio
import os
import random
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.node import MeshNode
from meshweaver.scheduler import SchedulingPolicy


def sample_batch_inside_circle(num_samples: int) -> int:
    """Worker function: draws random 2D points and counts hits inside unit circle."""
    hits = 0
    for _ in range(num_samples):
        x = random.random()
        y = random.random()
        if x * x + y * y <= 1.0:
            hits += 1
    return hits


def add_hits(a: int, b: int) -> int:
    """Reducer: sums hit counts."""
    return a + b


async def run_monte_carlo_demo():
    print("=== Starting MeshWeaver Distributed Monte Carlo Pi Estimation ===")

    total_samples = 1_000_000
    batch_count = 20
    samples_per_batch = total_samples // batch_count

    coordinator = MeshNode(host="127.0.0.1", udp_port=9200)
    await coordinator.start()

    try:
        print(f"Sampling {total_samples:,} points across {batch_count} distributed worker batches...")
        batch_sizes = [samples_per_batch] * batch_count

        # Step 1: Distributed map over batches
        results, batch_metrics = await coordinator.map(
            sample_batch_inside_circle,
            batch_sizes,
            concurrency=8,
            policy=SchedulingPolicy.LEAST_LOADED,
        )

        # Step 2: Distributed hierarchical tree reduction to sum hits
        total_hits = await coordinator.tree_reduce(
            reduce_fn=add_hits,
            data=results,
            initial_value=0,
            branching_factor=4,
        )

        pi_estimate = 4.0 * total_hits / total_samples

        print("\n--- Execution Summary ---")
        print(f"Total Points Sampled : {total_samples:,}")
        print(f"Total Circle Hits    : {total_hits:,}")
        print(f"Estimated Value of Pi: {pi_estimate:.6f}")
        print(f"Actual Value of Pi   : 3.141593")
        print(f"Error Percentage     : {abs(pi_estimate - 3.1415926535) / 3.1415926535 * 100:.4f}%")
        print(f"Sampling Duration    : {batch_metrics.duration_seconds:.4f}s")
        print(f"Throughput           : {total_samples / batch_metrics.duration_seconds:,.0f} points/sec")

    finally:
        await coordinator.stop()
        print("\n=== Demo Complete ===")


if __name__ == "__main__":
    asyncio.run(run_monte_carlo_demo())
