"""
SSL Context Caching for Mapepire Python.

This module provides SSL context caching to improve connection performance
by reusing SSL contexts for servers with identical SSL configurations.

 principle: Simple, thread-safe caching with
minimal complexity and maximum performance benefit.
"""

import hashlib
import os
import ssl
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Generic, Optional, TypeVar

from .data_types import DaemonServer

T = TypeVar("T")


@dataclass
class CachedItem(Generic[T]):
    """Cached item with metadata."""

    item: T
    created_at: float
    access_count: int
    last_accessed: float

    def is_expired(self, ttl_seconds: int) -> bool:
        """Check if item has expired based on TTL."""
        return time.time() - self.created_at > ttl_seconds


class GenericCache(Generic[T]):
    """Thread-safe generic cache with LRU eviction and TTL."""

    def __init__(self, max_size: int = 100, ttl_seconds: int = 3600):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache: OrderedDict[str, CachedItem[T]] = OrderedDict()
        self._lock = threading.RLock()
        self._stats = {"hits": 0, "misses": 0, "evictions": 0, "expirations": 0}

    def get(self, cache_key: str) -> Optional[CachedItem[T]]:
        """Get cached item if available and not expired."""
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is None:
                self._stats["misses"] += 1
                return None

            # Check if expired
            if cached.is_expired(self.ttl_seconds):
                del self._cache[cache_key]
                self._stats["expirations"] += 1
                self._stats["misses"] += 1
                return None

            # Update access info
            cached.access_count += 1
            cached.last_accessed = time.time()

            # Move to end (LRU)
            self._cache.move_to_end(cache_key)

            self._stats["hits"] += 1
            return cached

    def put(self, cache_key: str, item: T) -> None:
        """Store item in cache."""
        now = time.time()

        cached = CachedItem(
            item=item,
            created_at=now,
            access_count=0,  # Start at 0, will be incremented on first get()
            last_accessed=now,
        )

        with self._lock:
            # Remove oldest if at capacity
            if len(self._cache) >= self.max_size and cache_key not in self._cache:
                oldest_key, _ = self._cache.popitem(last=False)
                self._stats["evictions"] += 1

            self._cache[cache_key] = cached
            self._cache.move_to_end(cache_key)

    def clear(self) -> None:
        """Clear all cached items and reset statistics."""
        with self._lock:
            self._cache.clear()
            self._stats = {"hits": 0, "misses": 0, "evictions": 0, "expirations": 0}

    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        with self._lock:
            stats = self._stats.copy()
            stats["size"] = len(self._cache)
            return stats

    def cleanup_expired(self) -> int:
        """Remove expired items. Returns number of items removed."""
        removed = 0

        with self._lock:
            expired_keys = []
            for key, cached in self._cache.items():
                if cached.is_expired(self.ttl_seconds):
                    expired_keys.append(key)

            for key in expired_keys:
                del self._cache[key]
                removed += 1
                self._stats["expirations"] += 1

        return removed


# Cache key generators
def _get_ssl_context_key(server: DaemonServer) -> str:
    """Generate cache key for SSL contexts based on SSL-relevant server configuration."""
    # Only include SSL-relevant fields in cache key
    # Username/password don't affect SSL context
    ca_hash = ""
    if server.ca:
        if isinstance(server.ca, bytes):
            ca_hash = hashlib.sha256(server.ca).hexdigest()[:16]
        else:
            ca_hash = hashlib.sha256(server.ca.encode()).hexdigest()[:16]

    return f"ssl:{server.host}:{server.port}:{server.ignoreUnauthorized}:{ca_hash}"


def _get_certificate_key(server: DaemonServer) -> str:
    """Generate cache key for certificates based on server host and port only."""
    # Certificate depends only on the server, not credentials
    return f"cert:{server.host}:{server.port}"


# Global cache instances
_ssl_context_cache = GenericCache[ssl.SSLContext]()
_certificate_cache = GenericCache[bytes](max_size=50)


def should_use_cache(server: DaemonServer) -> bool:
    """Determine if SSL caching should be used for this server."""
    if server.ssl_cache_enabled is not None:
        return server.ssl_cache_enabled

    # Fall back to environment variable (default: False)
    return os.getenv("MAPEPIRE_SSL_CACHE", "false").lower() == "true"


# SSL Context caching functions
def get_cached_ssl_context(server: DaemonServer) -> Optional[ssl.SSLContext]:
    """Get cached SSL context if caching is enabled and context exists."""
    if not should_use_cache(server):
        return None

    cache_key = _get_ssl_context_key(server)
    cached = _ssl_context_cache.get(cache_key)
    return cached.item if cached else None


def cache_ssl_context(server: DaemonServer, context: ssl.SSLContext) -> None:
    """Cache SSL context if caching is enabled."""
    if should_use_cache(server):
        cache_key = _get_ssl_context_key(server)
        _ssl_context_cache.put(cache_key, context)


def get_ssl_cache_stats() -> Dict[str, int]:
    """Get SSL cache statistics."""
    return _ssl_context_cache.get_stats()


# Certificate caching functions
def get_cached_certificate(server: DaemonServer) -> Optional[bytes]:
    """Get cached certificate if caching is enabled and certificate exists."""
    if not should_use_cache(server):
        return None

    cache_key = _get_certificate_key(server)
    cached = _certificate_cache.get(cache_key)
    return cached.item if cached else None


def cache_certificate(server: DaemonServer, certificate: bytes) -> None:
    """Cache certificate if caching is enabled."""
    if should_use_cache(server):
        cache_key = _get_certificate_key(server)
        _certificate_cache.put(cache_key, certificate)


def get_certificate_cache_stats() -> Dict[str, int]:
    """Get certificate cache statistics."""
    return _certificate_cache.get_stats()


# Utility functions
def clear_ssl_cache() -> None:
    """Clear all cached SSL contexts."""
    _ssl_context_cache.clear()


def clear_certificate_cache() -> None:
    """Clear all cached certificates."""
    _certificate_cache.clear()


def cleanup_expired_ssl_contexts() -> int:
    """Remove expired SSL contexts. Returns number removed."""
    return _ssl_context_cache.cleanup_expired()


def cleanup_expired_certificates() -> int:
    """Remove expired certificates. Returns number removed."""
    return _certificate_cache.cleanup_expired()


# Backward compatibility aliases (deprecated)
@dataclass
class SSLCacheConfig:
    """Configuration for SSL context caching."""

    enabled: bool = False  # Disabled by default
    max_size: int = 100
    ttl_seconds: int = 3600  # 1 hour
    cleanup_interval: int = 300  # 5 minutes


# Maintain compatibility with existing tests
CachedSSLContext = CachedItem[ssl.SSLContext]
CachedCertificate = CachedItem[bytes]
SSLContextCache = GenericCache[ssl.SSLContext]
CertificateCache = GenericCache[bytes]
