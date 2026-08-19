"""
MeshWeaver Node Entry Point
"""

import asyncio
import os
import sys

# Support running from within meshweaver subfolder or root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

try:
    from meshweaver.meshweaver.node import MeshNode, cli_main
except ImportError:
    from meshweaver.node import MeshNode, cli_main

if __name__ == "__main__":
    asyncio.run(cli_main())
