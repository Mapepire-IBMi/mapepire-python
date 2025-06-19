"""
Proof of Concept: WebSocket Connection Reuse Performance Benefits

This POC demonstrates the performance improvement from reusing WebSocket connections
instead of creating a new connection for each SQLJob operation.
"""

import time
import threading
import uuid
from typing import Dict, Optional
from unittest.mock import MagicMock, patch
from dataclasses import dataclass
from statistics import mean

import pytest

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import DaemonServer


@dataclass
class MockConnectionMetrics:
    """Track connection creation and reuse metrics."""
    connections_created: int = 0
    connections_reused: int = 0
    total_connection_time: float = 0.0
    total_query_time: float = 0.0


class MockWebSocketConnection:
    """Mock WebSocket connection that simulates real connection overhead."""
    
    def __init__(self, server: DaemonServer, connection_delay: float = 1.0):
        self.server = server
        self.connection_delay = connection_delay
        self.is_connected = False
        self.created_at = time.time()
        self.last_used = time.time()
        self.query_count = 0
        
        # Simulate connection establishment time
        time.sleep(connection_delay)
        self.is_connected = True
        
        # Track metrics
        MockConnectionMetrics.connections_created += 1
        MockConnectionMetrics.total_connection_time += connection_delay
    
    def execute_query(self, sql: str, query_delay: float = 0.05):
        """Simulate query execution with minimal delay."""
        if not self.is_connected:
            raise RuntimeError("Not connected")
        
        self.last_used = time.time()
        self.query_count += 1
        
        # Simulate query processing time
        time.sleep(query_delay)
        MockConnectionMetrics.total_query_time += query_delay
        
        return {
            "success": True,
            "data": [{"result": f"Query {self.query_count} executed"}],
            "metadata": {"query": sql}
        }
    
    def close(self):
        """Close the connection."""
        self.is_connected = False


class WebSocketConnectionPool:
    """Simple connection pool for reusing WebSocket connections."""
    
    def __init__(self):
        self._connections: Dict[str, MockWebSocketConnection] = {}
        self._lock = threading.RLock()
    
    def _get_pool_key(self, server: DaemonServer) -> str:
        """Generate unique key for server configuration."""
        return f"{server.host}:{server.port}:{server.user}"
    
    def get_connection(self, server: DaemonServer, connection_delay: float = 1.0) -> MockWebSocketConnection:
        """Get pooled connection or create new one."""
        pool_key = self._get_pool_key(server)
        
        with self._lock:
            # Try to reuse existing connection
            if pool_key in self._connections:
                conn = self._connections[pool_key]
                if conn.is_connected:
                    MockConnectionMetrics.connections_reused += 1
                    return conn
                else:
                    # Remove dead connection
                    del self._connections[pool_key]
            
            # Create new connection
            conn = MockWebSocketConnection(server, connection_delay)
            self._connections[pool_key] = conn
            return conn
    
    def clear(self):
        """Clear all pooled connections."""
        with self._lock:
            for conn in self._connections.values():
                conn.close()
            self._connections.clear()
    
    def get_stats(self):
        """Get pool statistics."""
        with self._lock:
            return {
                "pooled_connections": len(self._connections),
                "active_connections": sum(1 for c in self._connections.values() if c.is_connected)
            }


# Global connection pool instance
_global_pool = WebSocketConnectionPool()


class SQLJobWithoutReuse(SQLJob):
    """Original SQLJob behavior - creates new connection each time."""
    
    def __init__(self, connection_delay: float = 1.0):
        super().__init__()
        self.connection_delay = connection_delay
        self._mock_connection = None
    
    def connect(self, server: DaemonServer):
        """Create new connection every time (original behavior)."""
        self._mock_connection = MockWebSocketConnection(server, self.connection_delay)
        return {"success": True}
    
    def query_and_run(self, sql: str):
        """Execute query with new connection."""
        if not self._mock_connection:
            raise RuntimeError("Not connected")
        return self._mock_connection.execute_query(sql)
    
    def close(self):
        """Close connection immediately."""
        if self._mock_connection:
            self._mock_connection.close()
            self._mock_connection = None


class SQLJobWithReuse(SQLJob):
    """Enhanced SQLJob with connection reuse."""
    
    def __init__(self, connection_delay: float = 1.0):
        super().__init__()
        self.connection_delay = connection_delay
        self._mock_connection = None
    
    def connect(self, server: DaemonServer):
        """Get pooled connection (reuse behavior)."""
        self._mock_connection = _global_pool.get_connection(server, self.connection_delay)
        return {"success": True}
    
    def query_and_run(self, sql: str):
        """Execute query with pooled connection."""
        if not self._mock_connection:
            raise RuntimeError("Not connected")
        return self._mock_connection.execute_query(sql)
    
    def close(self):
        """Return connection to pool instead of closing."""
        # Don't actually close - connection stays in pool
        self._mock_connection = None


def reset_metrics():
    """Reset all performance metrics."""
    MockConnectionMetrics.connections_created = 0
    MockConnectionMetrics.connections_reused = 0
    MockConnectionMetrics.total_connection_time = 0.0
    MockConnectionMetrics.total_query_time = 0.0
    _global_pool.clear()


def run_sequential_queries(sql_job_class, server: DaemonServer, num_queries: int = 5) -> Dict:
    """Run sequential queries and measure performance."""
    start_time = time.perf_counter()
    query_times = []
    
    for i in range(num_queries):
        query_start = time.perf_counter()
        
        with sql_job_class(connection_delay=0.5) as job:  # Reduced delay for faster testing
            job.connect(server)
            result = job.query_and_run(f"SELECT {i} as query_number")
            assert result["success"]
        
        query_time = time.perf_counter() - query_start
        query_times.append(query_time)
    
    total_time = time.perf_counter() - start_time
    
    return {
        "total_time": total_time,
        "average_query_time": mean(query_times),
        "query_times": query_times,
        "connections_created": MockConnectionMetrics.connections_created,
        "connections_reused": MockConnectionMetrics.connections_reused,
        "pool_stats": _global_pool.get_stats()
    }


@pytest.mark.performance
def test_websocket_connection_reuse_performance():
    """Compare performance between connection reuse and no reuse."""
    
    server = DaemonServer(
        host="test.example.com",
        user="testuser", 
        password="testpass",
        port=8443
    )
    
    num_queries = 8
    
    print(f"\n🔄 Performance Comparison: {num_queries} Sequential Queries")
    print("=" * 60)
    
    # Test WITHOUT connection reuse (original behavior)
    print("\n📊 Testing WITHOUT connection reuse (original SQLJob)...")
    reset_metrics()
    
    results_no_reuse = run_sequential_queries(SQLJobWithoutReuse, server, num_queries)
    
    print(f"  Total time: {results_no_reuse['total_time']:.3f}s")
    print(f"  Average per query: {results_no_reuse['average_query_time']:.3f}s")
    print(f"  Connections created: {results_no_reuse['connections_created']}")
    print(f"  Connections reused: {results_no_reuse['connections_reused']}")
    print(f"  Query times: {[f'{t:.3f}s' for t in results_no_reuse['query_times']]}")
    
    # Test WITH connection reuse (enhanced behavior)
    print(f"\n⚡ Testing WITH connection reuse (enhanced SQLJob)...")
    reset_metrics()
    
    results_with_reuse = run_sequential_queries(SQLJobWithReuse, server, num_queries)
    
    print(f"  Total time: {results_with_reuse['total_time']:.3f}s")
    print(f"  Average per query: {results_with_reuse['average_query_time']:.3f}s")
    print(f"  Connections created: {results_with_reuse['connections_created']}")
    print(f"  Connections reused: {results_with_reuse['connections_reused']}")
    print(f"  Pool stats: {results_with_reuse['pool_stats']}")
    print(f"  Query times: {[f'{t:.3f}s' for t in results_with_reuse['query_times']]}")
    
    # Calculate performance improvements
    time_improvement = results_no_reuse['total_time'] - results_with_reuse['total_time']
    time_improvement_percent = (time_improvement / results_no_reuse['total_time']) * 100
    
    avg_improvement = results_no_reuse['average_query_time'] - results_with_reuse['average_query_time']
    avg_improvement_percent = (avg_improvement / results_no_reuse['average_query_time']) * 100
    
    connection_reduction = results_no_reuse['connections_created'] - results_with_reuse['connections_created']
    
    print(f"\n📈 Performance Improvements:")
    print(f"  Total time saved: {time_improvement:.3f}s ({time_improvement_percent:.1f}% faster)")
    print(f"  Average query time saved: {avg_improvement:.3f}s ({avg_improvement_percent:.1f}% faster)")
    print(f"  Connections saved: {connection_reduction} ({connection_reduction/results_no_reuse['connections_created']*100:.1f}% reduction)")
    print(f"  Connection reuse rate: {results_with_reuse['connections_reused']}/{num_queries} queries ({results_with_reuse['connections_reused']/num_queries*100:.1f}%)")
    
    # Verify performance improvements
    assert results_with_reuse['total_time'] < results_no_reuse['total_time'], \
        "Connection reuse should be faster"
    
    assert results_with_reuse['connections_created'] < results_no_reuse['connections_created'], \
        "Connection reuse should create fewer connections"
    
    assert results_with_reuse['connections_reused'] > 0, \
        "Should have reused connections"
    
    # The first query should be similar (both create connection), 
    # but subsequent queries should be much faster with reuse
    first_query_diff = abs(results_no_reuse['query_times'][0] - results_with_reuse['query_times'][0])
    later_query_diff = abs(results_no_reuse['query_times'][-1] - results_with_reuse['query_times'][-1])
    
    assert later_query_diff > first_query_diff, \
        "Later queries should show bigger improvement due to connection reuse"
    
    print(f"\n✅ Performance test completed successfully!")
    print(f"   Connection reuse provides {time_improvement_percent:.1f}% performance improvement!")


@pytest.mark.performance  
def test_connection_reuse_with_multiple_servers():
    """Test connection reuse with multiple different servers."""
    
    server_a = DaemonServer(host="server-a.com", user="user", password="pass", port=8443)
    server_b = DaemonServer(host="server-b.com", user="user", password="pass", port=8443)
    
    print(f"\n🌐 Testing connection reuse with multiple servers...")
    reset_metrics()
    
    # Connect to server A multiple times
    for i in range(3):
        with SQLJobWithReuse(connection_delay=0.2) as job:
            job.connect(server_a)
            result = job.query_and_run(f"SELECT {i} FROM server_a")
            assert result["success"]
    
    # Connect to server B multiple times  
    for i in range(3):
        with SQLJobWithReuse(connection_delay=0.2) as job:
            job.connect(server_b)
            result = job.query_and_run(f"SELECT {i} FROM server_b")
            assert result["success"]
    
    # Back to server A - should reuse existing connection
    for i in range(2):
        with SQLJobWithReuse(connection_delay=0.2) as job:
            job.connect(server_a)
            result = job.query_and_run(f"SELECT {i} FROM server_a_again")
            assert result["success"]
    
    pool_stats = _global_pool.get_stats()
    
    print(f"  Connections created: {MockConnectionMetrics.connections_created}")
    print(f"  Connections reused: {MockConnectionMetrics.connections_reused}")
    print(f"  Pooled connections: {pool_stats['pooled_connections']}")
    print(f"  Active connections: {pool_stats['active_connections']}")
    
    # Should have created exactly 2 connections (one per server)
    assert MockConnectionMetrics.connections_created == 2, \
        f"Should create 2 connections (one per server), created {MockConnectionMetrics.connections_created}"
    
    # Should have reused connections 6 times (3+3-1 + 2-0 for server A, 3-1 for server B) 
    assert MockConnectionMetrics.connections_reused >= 6, \
        f"Should reuse connections at least 6 times, reused {MockConnectionMetrics.connections_reused}"
    
    # Should have 2 connections in pool
    assert pool_stats['pooled_connections'] == 2, \
        f"Should have 2 pooled connections, got {pool_stats['pooled_connections']}"
    
    print(f"✅ Multi-server connection reuse working correctly!")


@pytest.mark.performance
def test_connection_reuse_thread_safety():
    """Test that connection reuse is thread-safe."""
    
    server = DaemonServer(host="threaded.example.com", user="user", password="pass", port=8443)
    results = []
    errors = []
    
    def worker(worker_id: int):
        """Worker function to test concurrent connection reuse."""
        try:
            for i in range(3):
                with SQLJobWithReuse(connection_delay=0.1) as job:
                    job.connect(server)
                    result = job.query_and_run(f"SELECT {i} FROM worker_{worker_id}")
                    results.append(result["success"])
        except Exception as e:
            errors.append(e)
    
    print(f"\n🧵 Testing thread safety with connection reuse...")
    reset_metrics()
    
    # Start multiple threads
    threads = []
    num_threads = 5
    
    for worker_id in range(num_threads):
        thread = threading.Thread(target=worker, args=(worker_id,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    pool_stats = _global_pool.get_stats()
    
    print(f"  Threads: {num_threads}")
    print(f"  Total queries: {len(results)}")
    print(f"  Successful queries: {sum(results)}")
    print(f"  Errors: {len(errors)}")
    print(f"  Connections created: {MockConnectionMetrics.connections_created}")
    print(f"  Connections reused: {MockConnectionMetrics.connections_reused}")
    print(f"  Pooled connections: {pool_stats['pooled_connections']}")
    
    # Verify thread safety
    assert len(errors) == 0, f"Should have no errors, got: {errors}"
    assert all(results), "All queries should succeed"
    assert pool_stats['pooled_connections'] == 1, "Should have 1 pooled connection for same server"
    
    # Should create only 1 connection despite multiple threads
    assert MockConnectionMetrics.connections_created == 1, \
        f"Should create only 1 connection, created {MockConnectionMetrics.connections_created}"
    
    print(f"✅ Thread safety test passed!")


if __name__ == "__main__":
    # Run the POC directly
    pytest.main([__file__, "-v", "-s"])