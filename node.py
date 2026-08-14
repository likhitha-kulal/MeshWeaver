"""
MeshWeaver Node Entry Point
"""

import asyncio
from meshweaver.node import cli_main

if __name__ == "__main__":
    asyncio.run(cli_main())
