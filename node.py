"""
MeshWeaver Node CLI Entrypoint
"""

import argparse
import asyncio
import logging
from meshweaver.node import MeshNode

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


async def main():
    parser = argparse.ArgumentParser(description="MeshWeaver Distributed Mesh Node")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host interface to bind")
    parser.add_argument("--port", type=int, default=9000, help="UDP port to bind")
    parser.add_argument("--tcp-port", type=int, default=0, help="TCP port for task execution (0 for auto)")
    parser.add_argument("--bootstrap-host", type=str, default=None, help="Bootstrap peer host")
    parser.add_argument("--bootstrap-port", type=int, default=None, help="Bootstrap peer port")
    parser.add_argument("--enable-consensus", action="store_true", help="Enable distributed leader election engine")
    parser.add_argument("--enable-raft", action="store_true", default=True, help="Enable Raft replicated state machine")
    parser.add_argument("--replicated-state-demo", action="store_true", help="Run replicated state machine demo")

    args = parser.parse_args()

    bootstraps = []
    if args.bootstrap_host and args.bootstrap_port:
        bootstraps.append((args.bootstrap_host, args.bootstrap_port))

    node = MeshNode(
        host=args.host,
        port=args.port,
        tcp_port=args.tcp_port,
        bootstrap_nodes=bootstraps,
        enable_consensus=args.enable_consensus,
        enable_raft=args.enable_raft,
    )
    await node.start()

    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
