"""
MeshWeaver Distributed Consensus & Leader Election Interactive Demonstration.
Demonstrates live cluster leader election, periodic lease heartbeat renewals,
graceful leader failover, and real-time ASCII consensus dashboard.
"""

import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from meshweaver.models import NodeID
from meshweaver.node import MeshNode

logging.basicConfig(level=logging.WARNING)


def render_dashboard(nodes, iteration_desc=""):
    print("\n" + "=" * 78)
    print(f"  [*] MESHWEAVER CONSENSUS DASHBOARD: {iteration_desc}")
    print("=" * 78)
    print(f"{'Node ID':<18} | {'Port':<8} | {'Role':<12} | {'Term':<6} | {'Leader ID':<18} | {'Heartbeats'}")
    print("-" * 78)
    for n in nodes:
        metrics = n.get_consensus_metrics()
        role_badge = f"[LEADER]" if metrics.is_leader else f" {metrics.role}"
        leader_short = (metrics.current_leader[:8] + "...") if metrics.current_leader else "None"
        hb_stat = f"S:{metrics.heartbeats_sent} / R:{metrics.heartbeats_received}"
        print(f"{n.node_id.hex()[:14]}.. | {n.bound_udp_port:<8} | {role_badge:<12} | {metrics.current_term:<6} | {leader_short:<18} | {hb_stat}")
    print("=" * 78 + "\n")


async def main():
    print("\n[+] Starting 3-Node MeshWeaver Cluster for Distributed Leader Election Demo...")
    
    node1 = MeshNode(host="127.0.0.1", udp_port=9600, tcp_port=9601)
    node2 = MeshNode(host="127.0.0.1", udp_port=9610, tcp_port=9611)
    node3 = MeshNode(host="127.0.0.1", udp_port=9620, tcp_port=9621)
    nodes = [node1, node2, node3]
    
    for n in nodes:
        await n.start()
        n.leader_election.config.min_election_timeout = 0.200
        n.leader_election.config.max_election_timeout = 0.400
        n.leader_election.config.heartbeat_interval = 0.080

    try:
        print("[+] Bootstrapping cluster nodes (Node 2 & Node 3 joining Node 1)...")
        await node2.bootstrap([("127.0.0.1", 9600)])
        await node3.bootstrap([("127.0.0.1", 9600)])
        await node1.bootstrap([("127.0.0.1", 9610)])
        await asyncio.sleep(0.3)
        
        render_dashboard(nodes, "Initial Follower State")
        
        print("[+] Triggering Candidate Election from Node 1...")
        await node1.trigger_election()
        await asyncio.sleep(0.5)
        
        render_dashboard(nodes, "Leader Elected & Heartbeats Active")
        
        active_leader = next((n for n in nodes if n.is_leader), None)
        if active_leader:
            print(f"[!] Simulating Sudden Crash/Failure of Current Leader ({active_leader.node_id.hex()[:8]}...)...")
            nodes.remove(active_leader)
            await active_leader.stop()
            
            # Clean up dead node reference
            for remaining in nodes:
                remaining.routing_table.remove_contact(active_leader.node_id)
                remaining.gossip_manager.neighbors.pop(active_leader.node_id.hex(), None)
                remaining.gossip_manager.peer_loads.pop(active_leader.node_id.hex(), None)
            
            print("[+] Remaining nodes detecting heartbeat timeout and initiating re-election...")
            if nodes:
                await nodes[0].trigger_election()
            await asyncio.sleep(0.6)
            
            render_dashboard(nodes, "Post-Failover Cluster (New Leader Elected)")
            new_leader = next((n for n in nodes if n.is_leader), None)
            if new_leader:
                print(f"[OK] Success! New Leader {new_leader.node_id.hex()[:8]} has smoothly taken over leadership.")

    finally:
        print("[*] Shutting down cluster demo nodes...")
        for n in list(nodes):
            try:
                await n.stop()
            except Exception:
                pass
        print("[OK] Demo finished successfully!\n")


if __name__ == "__main__":
    asyncio.run(main())
