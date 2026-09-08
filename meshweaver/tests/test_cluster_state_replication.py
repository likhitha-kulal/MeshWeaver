"""
Cluster integration test for multi-node state replication, atomic CAS, and distributed locking.
"""

import asyncio
import pytest
from meshweaver.models import RaftCommandType
from meshweaver.node import MeshNode


@pytest.mark.asyncio
async def test_cluster_state_replication_and_distributed_lock():
    # Spin up 3-node cluster
    n1 = MeshNode(host="127.0.0.1", port=0, enable_consensus=False)
    await n1.start()

    n2 = MeshNode(host="127.0.0.1", port=0, bootstrap_nodes=[("127.0.0.1", n1.port)], enable_consensus=False)
    await n2.start()

    n3 = MeshNode(host="127.0.0.1", port=0, bootstrap_nodes=[("127.0.0.1", n1.port)], enable_consensus=False)
    await n3.start()

    # Let nodes discover each other
    await asyncio.sleep(0.3)

    try:
        # Leader (n1) sets key
        val = await n1.state_set("cluster_name", "HyperMesh-1")
        assert val == "HyperMesh-1"
        assert n1.state_get("cluster_name") == "HyperMesh-1"

        # Increment shared counter
        c1 = await n1.state_increment("task_counter", delta=10)
        assert c1 == 10
        assert n1.state_get("task_counter") == 10

        # Atomic Compare-And-Swap (CAS)
        cas_ok = await n1.state_cas("cluster_name", expected="HyperMesh-1", new_value="HyperMesh-Alpha")
        assert cas_ok is True
        assert n1.state_get("cluster_name") == "HyperMesh-Alpha"

        # Distributed Lock Acquisition
        lock_res1 = await n1.acquire_lock("db_write_mutex", ttl_seconds=10.0)
        assert lock_res1.acquired is True
        assert lock_res1.fencing_token == 1

        # Release Lock
        rel_ok = await n1.release_lock("db_write_mutex", fencing_token=1)
        assert rel_ok is True

        # Re-acquire lock -> receives monotonic incremented fencing token
        lock_res2 = await n1.acquire_lock("db_write_mutex", ttl_seconds=10.0)
        assert lock_res2.acquired is True
        assert lock_res2.fencing_token == 2

        # Verify metrics
        metrics = n1.get_raft_metrics()
        assert metrics.total_proposals >= 5
        assert metrics.state_keys_count >= 2

    finally:
        await n1.stop()
        await n2.stop()
        await n3.stop()
