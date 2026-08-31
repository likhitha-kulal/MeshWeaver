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

    async def get(self, cache_key: str, timeout: float = 5.0) -> Optional[Any]:
        """Query DHT storage for a memoized result."""
        if not self.dht_storage:
            return None

        try:
            val = await self.dht_storage.find_value(cache_key, timeout=timeout)
            if val is not None:
                logger.info(f"DHT TaskCache HIT for key {cache_key[:24]}...")
                return val
        except Exception as e:
            logger.warning(f"DHT TaskCache read error for {cache_key[:24]}...: {e}")
        return None

    async def put(self, cache_key: str, value: Any, ttl: Optional[float] = None, timeout: float = 5.0) -> bool:
        """Store a computed result in the DHT with TTL."""
        if not self.dht_storage:
            return False

        effective_ttl = ttl if ttl is not None else self.default_ttl
        try:
            stored_nodes = await self.dht_storage.store(cache_key, value, ttl=effective_ttl, timeout=timeout)
            logger.info(f"DHT TaskCache stored on {stored_nodes} peers for key {cache_key[:24]}... (TTL={effective_ttl}s)")
            return stored_nodes > 0
        except Exception as e:
            logger.warning(f"DHT TaskCache write error for {cache_key[:24]}...: {e}")
            return False

