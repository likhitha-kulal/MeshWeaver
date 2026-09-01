"""
MeshWeaver Example: Distributed MapReduce Word Count
Demonstrates distributed word frequency calculation across peer nodes.
"""

import asyncio
import os
import sys
from typing import List, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.node import MeshNode
from meshweaver.scheduler import SchedulingPolicy


SAMPLE_CORPUS = [
    "To be or not to be that is the question",
    "Whether tis nobler in the mind to suffer",
    "The slings and arrows of outrageous fortune",
    "Or to take arms against a sea of troubles",
    "And by opposing end them to die to sleep",
    "No more and by a sleep to say we end",
    "The heart-ache and the thousand natural shocks",
    "That flesh is heir to tis a consummation",
    "Devoutly to be wish'd To die to sleep",
    "To sleep perchance to dream ay there's the rub",
]


def tokenize_mapper(line: str) -> List[Tuple[str, int]]:
    """Mapper function: transforms a line of text into (word, 1) tuples."""
    words = line.lower().replace("'", "").replace("-", " ").split()
    return [(w.strip(".,!?:;\""), 1) for w in words if w.strip(".,!?:;\"")]


def sum_reducer(word: str, counts: List[int]) -> int:
    """Reducer function: sums word count instances."""
    return sum(counts)


async def run_word_count_demo():
    print("=== Starting MeshWeaver Distributed Word Count Demo ===")

    # Initialize a local coordinator node
    coordinator = MeshNode(host="127.0.0.1", udp_port=9100)
    await coordinator.start()

    try:
        print(f"Processing corpus of {len(SAMPLE_CORPUS)} lines across mesh cluster...")
        word_counts, metrics = await coordinator.map_reduce(
            map_fn=tokenize_mapper,
            reduce_fn=sum_reducer,
            data=SAMPLE_CORPUS,
            chunk_size=2,
            policy=SchedulingPolicy.LEAST_LOADED,
        )

        print("\n--- Execution Metrics ---")
        print(f"Total Input Lines     : {metrics.total_input_items}")
        print(f"Total Word Pairs      : {metrics.total_intermediate_pairs}")
        print(f"Unique Partitions/Keys: {metrics.total_partitions}")
        print(f"Map Phase Duration    : {metrics.map_duration_seconds:.4f}s")
        print(f"Shuffle Phase Duration: {metrics.shuffle_duration_seconds:.4f}s")
        print(f"Reduce Phase Duration : {metrics.reduce_duration_seconds:.4f}s")
        print(f"Total Workflow Time   : {metrics.total_duration_seconds:.4f}s")
        print(f"Cluster Throughput    : {metrics.throughput_items_per_sec:.1f} lines/sec")

        print("\n--- Top 10 Most Frequent Words ---")
        top_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        for rank, (word, count) in enumerate(top_words, 1):
            print(f"  {rank:>2}. '{word}': {count}")

    finally:
        await coordinator.stop()
        print("\n=== Demo Complete ===")


if __name__ == "__main__":
    asyncio.run(run_word_count_demo())
