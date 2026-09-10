"""
Unit tests for the live ASCII ClusterTelemetryDashboard renderer.
"""

import asyncio
import pytest
from meshweaver.dashboard import ClusterTelemetryDashboard
from meshweaver.node import MeshNode
from meshweaver.models import (
    DistributedBarrierSpec,
    DistributedSemaphoreSpec,
    StorageConfig,
)


@pytest.mark.anyio
async def test_dashboard_render_empty_and_active_nodes(tmp_path):
    dashboard = ClusterTelemetryDashboard(use_ansi_color=False)

    # Empty nodes
    frame_empty = dashboard.render_frame([])
    assert "MESHWEAVER DISTRIBUTED COMPUTE MESH - LIVE TELEMETRY" in frame_empty
    assert "Nodes: 0" in frame_empty

    # Active nodes
    cfg1 = StorageConfig(data_dir=str(tmp_path / "n1"), node_storage_id="dash1")
    cfg2 = StorageConfig(data_dir=str(tmp_path / "n2"), node_storage_id="dash2")

    node1 = MeshNode(host="127.0.0.1", udp_port=9701, tcp_port=9702, storage_config=cfg1)
    node2 = MeshNode(host="127.0.0.1", udp_port=9703, tcp_port=9704, storage_config=cfg2)

    await node1.start()
    await node2.start()

    try:
        # Register a barrier and semaphore
        node1.synchronization_manager.get_or_create_barrier("b-dash", 2)
        node1.synchronization_manager.get_or_create_semaphore("s-dash", 3)

        # Record a transaction
        tx = await node1.transaction_coordinator.begin_transaction()
        tx.write_set["key1"] = "val1"
        node1.transaction_coordinator._active_txs[tx.tx_id] = tx

        frame = dashboard.render_frame([node1, node2])
        assert node1.node_id.hex()[:8] in frame
        assert node2.node_id.hex()[:8] in frame
        assert "b-dash" in frame
        assert "s-dash" in frame
        assert "key1" in frame
        assert "ONLINE" in frame
    finally:
        await node1.stop()
        await node2.stop()


@pytest.mark.anyio
async def test_dashboard_run_live_duration(tmp_path):
    dashboard = ClusterTelemetryDashboard(use_ansi_color=True)
    cfg1 = StorageConfig(data_dir=str(tmp_path / "nlive"), node_storage_id="dashlive")
    node1 = MeshNode(host="127.0.0.1", udp_port=9705, tcp_port=9706, storage_config=cfg1)
    await node1.start()
    try:
        # Run for 0.1s
        await dashboard.run_live([node1], refresh_rate=0.05, duration=0.1)
    finally:
        await node1.stop()
