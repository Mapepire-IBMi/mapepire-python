"""
Simple WebSocket Connection Pool for Mapepire Python.

Simple, effective connection reuse with minimal complexity.
Transparently improves performance by reusing WebSocket connections across SQLJob instances.
"""

import atexit
import os
import signal
import threading
import time
import uuid
import weakref
from typing import Dict, Optional

from websockets.sync.client import ClientConnection

from .client.websocket_client import WebsocketConnection
from .data_types import DaemonServer


class PooledConnection:
    """A pooled WebSocket connection with metadata."""

    def __init__(self, server: DaemonServer, connection: ClientConnection):
        self.server = server
        self.connection = connection
        self.created_at = time.time()
        self.last_used = time.time()
        self.use_count = 0
        self.in_use = False

    def mark_used(self):
        """Mark connection as used."""
        self.last_used = time.time()
        self.use_count += 1
        self.in_use = True

    def mark_available(self):
        """Mark connection as available."""
        self.in_use = False

    def is_healthy(self) -> bool:
        """Check if connection is still healthy."""
        try:
            return (
                self.connection
                and not self.connection.closed
                and time.time() - self.last_used < 600  # 10 min timeout
            )
        except:
            return False

    def should_cleanup(self) -> bool:
        """Check if connection should be cleaned up."""
        age = time.time() - self.created_at
        idle_time = time.time() - self.last_used
        return (
            not self.is_healthy()
            or age > 3600  # 1 hour max age
            or idle_time > 600  # 10 min idle timeout
        )


class WebSocketConnectionPool:
    """Simple connection pool for WebSocket connections."""

    def __init__(self):
        self._connections: Dict[str, PooledConnection] = {}
        self._lock = threading.RLock()
        self._creation_locks: Dict[str, threading.Lock] = {}  # Per-server creation locks
        self._shutdown_event = threading.Event()
        self._active_jobs = weakref.WeakSet()  # Track active SQLJob instances

        # Register cleanup handlers
        atexit.register(self._cleanup_all)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _get_pool_key(self, server: DaemonServer, connection_id: Optional[str] = None) -> str:
        """Generate unique key for server configuration, optionally with connection ID."""
        base_key = f"{server.host}:{server.port}:{server.user}"
        if connection_id:
            return f"{base_key}:{connection_id}"
        return base_key

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self._cleanup_all()

    def _cleanup_all(self):
        """Clean up all connections on shutdown."""
        self._shutdown_event.set()

        with self._lock:
            for conn in list(self._connections.values()):
                try:
                    if conn.connection and not conn.connection.closed:
                        conn.connection.close()
                except:
                    pass  # Ignore errors during shutdown
            self._connections.clear()

    def _find_available_connection(self, base_key: str) -> Optional[ClientConnection]:
        """Find available connection for server (assumes lock is held)."""
        available_connections = [
            (key, conn)
            for key, conn in self._connections.items()
            if key.startswith(base_key) and conn.is_healthy() and not conn.in_use
        ]

        if available_connections:
            # Use least recently used connection for better distribution
            key, pooled = min(available_connections, key=lambda x: x[1].last_used)
            pooled.mark_used()
            return pooled.connection

        return None

    def get_connection(self, server: DaemonServer) -> ClientConnection:
        """Get pooled connection with improved concurrency."""
        if self._shutdown_event.is_set():
            # During shutdown, create direct connection
            socket = WebsocketConnection(server)
            return socket.connect()

        base_key = self._get_pool_key(server)

        # Fast path: check for available connections with minimal locking
        with self._lock:
            available = self._find_available_connection(base_key)
            if available:
                return available

        # Slow path: create new connection with per-server lock to reduce contention
        creation_lock = self._creation_locks.setdefault(base_key, threading.Lock())

        with creation_lock:  # Only block other requests to same server
            # Double-check after acquiring creation lock
            with self._lock:
                available = self._find_available_connection(base_key)
                if available:
                    return available

                # Check if we can create more connections for this server
                server_connections = sum(
                    1 for k in self._connections.keys() if k.startswith(base_key)
                )
                max_per_server = int(os.getenv("MAPEPIRE_MAX_CONNECTIONS_PER_SERVER", "3"))

                if server_connections >= max_per_server:
                    # Use least busy existing connection
                    server_conns = [
                        (key, conn)
                        for key, conn in self._connections.items()
                        if key.startswith(base_key) and conn.is_healthy()
                    ]
                    if server_conns:
                        key, pooled = min(server_conns, key=lambda x: x[1].use_count)
                        pooled.mark_used()
                        return pooled.connection
                    else:
                        raise RuntimeError("No healthy connections available")

            # Create new connection outside main lock to reduce contention
            socket = WebsocketConnection(server)
            connection = socket.connect()

            # Store in pool with unique connection ID
            connection_id = str(uuid.uuid4())[:8]
            pool_key = self._get_pool_key(server, connection_id)

            with self._lock:
                pooled = PooledConnection(server, connection)
                pooled.mark_used()
                self._connections[pool_key] = pooled

            return connection

    def return_connection(self, server: DaemonServer, connection: ClientConnection):
        """Return connection to pool."""
        base_key = self._get_pool_key(server)

        with self._lock:
            # Find the specific connection in the pool
            for key, pooled in self._connections.items():
                if key.startswith(base_key) and pooled.connection is connection:
                    pooled.mark_available()
                    break

    def _remove_connection(self, pool_key: str):
        """Remove connection from pool."""
        if pool_key in self._connections:
            pooled = self._connections.pop(pool_key)
            try:
                if pooled.connection and not pooled.connection.closed:
                    pooled.connection.close()
            except:
                pass  # Ignore errors during cleanup

    def cleanup_idle_connections(self):
        """Clean up idle and expired connections."""
        with self._lock:
            to_remove = []
            for key, pooled in self._connections.items():
                if not pooled.in_use and pooled.should_cleanup():
                    to_remove.append(key)

            for key in to_remove:
                self._remove_connection(key)

        return len(to_remove)

    def get_stats(self) -> Dict[str, int]:
        """Get pool statistics."""
        with self._lock:
            total = len(self._connections)
            active = sum(1 for c in self._connections.values() if c.in_use)
            healthy = sum(1 for c in self._connections.values() if c.is_healthy())

            return {
                "total_connections": total,
                "active_connections": active,
                "healthy_connections": healthy,
                "idle_connections": total - active,
            }

    def register_job(self, job):
        """Register an active SQLJob instance."""
        self._active_jobs.add(job)

    def is_enabled(self) -> bool:
        """Check if connection pooling is enabled."""
        import os

        return os.getenv("MAPEPIRE_CONNECTION_POOL", "false").lower() == "true"


# Global connection pool instance
_connection_pool = WebSocketConnectionPool()


def get_pooled_connection(server: DaemonServer) -> ClientConnection:
    """Get a pooled WebSocket connection."""
    if _connection_pool.is_enabled():
        return _connection_pool.get_connection(server)
    else:
        # Fall back to direct connection
        socket = WebsocketConnection(server)
        return socket.connect()


def return_pooled_connection(server: DaemonServer, connection: ClientConnection):
    """Return a WebSocket connection to the pool."""
    if _connection_pool.is_enabled():
        _connection_pool.return_connection(server, connection)
    else:
        # Close connection immediately if pooling disabled
        try:
            if connection and not connection.closed:
                connection.close()
        except:
            pass


def get_pool_stats() -> Dict[str, int]:
    """Get connection pool statistics."""
    return _connection_pool.get_stats()


def cleanup_idle_connections() -> int:
    """Clean up idle connections. Returns number of connections cleaned up."""
    return _connection_pool.cleanup_idle_connections()


def register_active_job(job):
    """Register an active SQLJob instance for tracking."""
    _connection_pool.register_job(job)
