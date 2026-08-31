"""
MeshWeaver Task Cache Module.
Provides distributed memoization and caching of deterministic task computation
results over the Kademlia DHT.
"""

import hashlib
import json
import logging
from typing import Any, Callable, Optional

import cloudpickle
from meshweaver.dht_storage import DHTStorage

logger = logging.getLogger("meshweaver.task_cache")


class TaskCache:
    """
    Manages DHT-backed caching for remote task outputs to prevent redundant computation.
    """

    CACHE_KEY_PREFIX = "dht:task_cache:"

    def __init__(self, dht_storage: Optional[DHTStorage] = None, default_ttl: float = 300.0):
        self.dht_storage = dht_storage
        self.default_ttl = default_ttl

    @classmethod
    def compute_cache_key(cls, func: Callable, *args: Any, **kwargs: Any) -> str:
        """
        Generate a deterministic SHA-256 cache key from the function code/name and its arguments.
        """
        try:
            func_identifier = getattr(func, "__qualname__", getattr(func, "__name__", str(func)))
            func_code = getattr(func, "__code__", None)
            code_bytes = func_code.co_code if func_code else func_identifier.encode("utf-8")
        except Exception:
            code_bytes = str(func).encode("utf-8")

        args_bytes = cloudpickle.dumps((args, sorted(kwargs.items())))
        hasher = hashlib.sha256()
        hasher.update(code_bytes)
        hasher.update(args_bytes)
        digest = hasher.hexdigest()
        return f"{cls.CACHE_KEY_PREFIX}{digest}"
