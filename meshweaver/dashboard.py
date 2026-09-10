"""
Live interactive ASCII/Unicode Cluster Telemetry Dashboard for MeshWeaver.

Provides real-time visualization of:
- Raft Consensus topology, cluster terms, leader elections, and log indexes.
- Dynamic Backpressure & Adaptive Load Shedder watermarks ($W$) and QoS status tiers.
- Distributed 2-Phase Commit (2PC) ACID transactions and Optimistic Concurrency Control (OCC) locks.
- Distributed Synchronization Primitives (Barriers, Countdown Latches, Leased Semaphores).
- Write-Ahead Log (WAL) storage engine throughput and CRC32 framing integrity.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import List, Optional, Dict, Any

from meshweaver.node import MeshNode


class ClusterTelemetryDashboard:
    """
    ASCII / ANSI Telemetry Dashboard renderer for multi-node MeshWeaver clusters.
    """

    def __init__(self, use_ansi_color: bool = True) -> None:
        self.use_ansi_color = use_ansi_color

    def _colorize(self, text: str, color_code: str) -> str:
        if not self.use_ansi_color:
            return text
        return f"\033[{color_code}m{text}\033[0m"

    def green(self, text: str) -> str:
        return self._colorize(text, "32;1")

    def red(self, text: str) -> str:
        return self._colorize(text, "31;1")

    def yellow(self, text: str) -> str:
        return self._colorize(text, "33;1")

    def cyan(self, text: str) -> str:
        return self._colorize(text, "36;1")

    def bold(self, text: str) -> str:
        return self._colorize(text, "1")

    def dim(self, text: str) -> str:
        return self._colorize(text, "2")

    def render_frame(self, nodes: List[MeshNode]) -> str:
        """
        Renders a single frame of cluster telemetry as an ASCII string.
        """
        lines: List[str] = []
        width = 100

        # Header Box
        header_title = " MESHWEAVER DISTRIBUTED COMPUTE MESH - LIVE TELEMETRY "
        lines.append("╔" + "═" * (width - 2) + "╗")
        lines.append(f"║{self.cyan(header_title.center(width - 2))}║")
        
        # Summary row
        now_str = time.strftime("%Y-%m-%d %H:%M:%S")
        total_nodes = len(nodes)
        leaders = [n for n in nodes if getattr(n, "is_leader", False)]
        leader_id = leaders[0].node_id if leaders else "None (Electing...)"
        current_term = max((getattr(n.raft, "current_term", 0) for n in nodes), default=0) if nodes else 0

        summary_line = f" Time: {now_str}  │  Nodes: {total_nodes}  │  Leader: {leader_id}  │  Term: {current_term} "
        lines.append(f"║{summary_line.ljust(width - 2)}║")
        lines.append("╠" + "═" * (width - 2) + "╣")

        # Section 1: Node Topology & Consensus
        lines.append(f"║ {self.bold('NODE TOPOLOGY & RAFT CONSENSUS STATE')}".ljust(width + 5) + "║")
        lines.append(
            "║  "
            + f"{'Node ID':<16} {'Role':<12} {'Endpoint':<22} {'Term':<6} {'Log Idx':<9} {'Commit Idx':<12} {'State':<10}"
            + " ║"
        )
        lines.append("║  " + "─" * 94 + "  ║")

        for n in nodes:
            nid = n.node_id
            role = n.state.value if hasattr(n, "state") and hasattr(n.state, "value") else str(n.state)
            if role.upper() == "LEADER":
                role_str = self.green(f"★ {role}")
            elif role.upper() == "CANDIDATE":
                role_str = self.yellow(f"◇ {role}")
            else:
                role_str = self.dim(f"• {role}")

            endpoint = f"{n.host}:{n.port}"
            term = str(getattr(n.raft, "current_term", 0))
            log_idx = str(getattr(n.raft.log, "last_index", 0))
            commit_idx = str(getattr(n.raft, "commit_index", 0))
            active_str = self.green("ONLINE") if getattr(n, "_running", False) else self.red("STOPPED")

            row = f"  {nid:<16} {role_str:<21} {endpoint:<22} {term:<6} {log_idx:<9} {commit_idx:<12} {active_str:<19}"
            lines.append(f"║{row}║")

        lines.append("╠" + "═" * (width - 2) + "╣")

        # Section 2: Adaptive Backpressure & Watermark Engine
        lines.append(f"║ {self.bold('ADAPTIVE LOAD SHEDDER & BACKPRESSURE GAUGES')}".ljust(width + 5) + "║")
        lines.append(
            "║  "
            + f"{'Node ID':<16} {'Watermark (W)':<16} {'Status Tier':<14} {'CPU %':<8} {'Mem %':<8} {'In-Flight':<12} {'Tokens':<10}"
            + " ║"
        )
        lines.append("║  " + "─" * 94 + "  ║")

        for n in nodes:
            shedder = getattr(n, "load_shedder", None)
            if shedder:
                w_val = shedder.calculate_watermark()
                status = shedder.current_status.value
                if status == "NORMAL":
                    status_fmt = self.green(f"[NORMAL]")
                elif status == "WARNING":
                    status_fmt = self.yellow(f"[WARNING]")
                elif status == "CRITICAL":
                    status_fmt = self.red(f"[CRITICAL]")
                else:
                    status_fmt = self.red(f"[SHEDDING]")

                cpu = f"{shedder.metrics.cpu_utilization * 100:.1f}%"
                mem = f"{shedder.metrics.memory_utilization * 100:.1f}%"
                inflight = str(shedder.metrics.in_flight_requests)
                tokens = f"{shedder._tokens:.1f}"
                w_str = f"{w_val:.3f}"
            else:
                w_str, status_fmt, cpu, mem, inflight, tokens = "N/A", "N/A", "N/A", "N/A", "N/A", "N/A"

            row = f"  {n.node_id:<16} {w_str:<16} {status_fmt:<23} {cpu:<8} {mem:<8} {inflight:<12} {tokens:<10}"
            lines.append(f"║{row}║")

        lines.append("╠" + "═" * (width - 2) + "╣")

        # Section 3: 2PC Distributed Transactions & Synchronization
        lines.append(f"║ {self.bold('DISTRIBUTED 2PC TRANSACTIONS & SYNCHRONIZATION BARRIERS')}".ljust(width + 5) + "║")

        # Aggregate transactions across all nodes
        all_txs: Dict[str, Any] = {}
        for n in nodes:
            tx_coord = getattr(n, "tx_coordinator", None)
            if tx_coord:
                all_txs.update(tx_coord._active_txs)

        if all_txs:
            lines.append(
                "║  "
                + f"{'Tx ID':<24} {'Status':<14} {'Isolation':<14} {'Keys In Write-Set':<24} {'Coordinator':<14}"
                + " ║"
            )
            lines.append("║  " + "─" * 94 + "  ║")
            for tx_id, tx_ctx in list(all_txs.items())[:5]:
                st = tx_ctx.status.value
                if st == "COMMITTED":
                    st_fmt = self.green(st)
                elif st in ("ABORTED", "FAILED"):
                    st_fmt = self.red(st)
                else:
                    st_fmt = self.yellow(st)

                iso = tx_ctx.isolation_level.value
                keys = ",".join(list(tx_ctx.write_set.keys())) if tx_ctx.write_set else "(empty)"
                if len(keys) > 22:
                    keys = keys[:19] + "..."
                coord = getattr(tx_ctx, "coordinator_id", "local")
                row = f"  {tx_id:<24} {st_fmt:<23} {iso:<14} {keys:<24} {coord:<14}"
                lines.append(f"║{row}║")
        else:
            lines.append(f"║  {self.dim('No active or historical 2PC transactions recorded.')}".ljust(width + 5) + "║")

        lines.append("║" + " " * (width - 2) + "║")

        # Synchronization primitives overview
        lines.append(f"║  {self.bold('Active Synchronization Primitives:')}".ljust(width + 5) + "║")
        sync_items = []
        for n in nodes:
            sync_mgr = getattr(n, "sync_manager", None)
            if sync_mgr:
                for b_name, b in sync_mgr._barriers.items():
                    sync_items.append(f"Barrier '{b_name}' ({len(b._arrived_parties)}/{b.spec.parties_count} arrived)")
                for l_name, l in sync_mgr._latches.items():
                    sync_items.append(f"Latch '{l_name}' (count={l.count})")
                for s_name, s in sync_mgr._semaphores.items():
                    sync_items.append(f"Semaphore '{s_name}' (avail={s.available_permits()}/{s.spec.total_permits})")

        if sync_items:
            for item in set(sync_items)[:4]:
                lines.append(f"║    • {item}".ljust(width - 1) + "║")
        else:
            lines.append(f"║    • {self.dim('No barriers or semaphores currently registered.')}".ljust(width + 5) + "║")

        lines.append("╠" + "═" * (width - 2) + "╣")

        # Section 4: WAL Engine & Crash Recovery Storage
        lines.append(f"║ {self.bold('WRITE-AHEAD LOG (WAL) & STORAGE DURABILITY')}".ljust(width + 5) + "║")
        lines.append(
            "║  "
            + f"{'Node ID':<16} {'Fsync Mode':<14} {'WAL Records':<14} {'WAL Bytes':<14} {'Segments':<12} {'Recovery':<18}"
            + " ║"
        )
        lines.append("║  " + "─" * 94 + "  ║")

        for n in nodes:
            wal = getattr(n, "wal_engine", None)
            if wal:
                mode = wal.config.fsync_mode.value
                recs = str(wal.records_written)
                b_written = f"{wal.bytes_written} B"
                segs = str(len(wal.list_segments()))
                recov = self.green("ACTIVE / DURABLE")
            else:
                mode, recs, b_written, segs, recov = "N/A", "0", "0 B", "0", self.dim("DISABLED")

            row = f"  {n.node_id:<16} {mode:<14} {recs:<14} {b_written:<14} {segs:<12} {recov:<27}"
            lines.append(f"║{row}║")

        # Footer Box
        lines.append("╚" + "═" * (width - 2) + "╝")

        return "\n".join(lines)

    async def run_live(
        self,
        nodes: List[MeshNode],
        refresh_rate: float = 1.0,
        duration: Optional[float] = None,
    ) -> None:
        """
        Runs a live telemetry rendering loop clearing the screen on each tick.
        """
        start_time = time.monotonic()
        try:
            while True:
                frame = self.render_frame(nodes)
                # Clear screen (ANSI escape code)
                sys.stdout.write("\033[2J\033[H")
                sys.stdout.write(frame + "\n")
                sys.stdout.flush()

                if duration is not None and (time.monotonic() - start_time) >= duration:
                    break

                await asyncio.sleep(refresh_rate)
        except asyncio.CancelledError:
            pass
