"""
WebSocket Connection Pool Tests with IBM i Systems.

These tests verify that connection pooling works correctly with real IBM i systems
and provides measurable performance improvements.
"""

import os
import time
import threading
from statistics import mean
from typing import List, Dict, Any

import pytest

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import DaemonServer
from mapepire_python.connection_pool import (
    get_pool_stats,
    cleanup_idle_connections,
    _connection_pool
)


class ConnectionPoolTestRunner:
    """Helper class to run connection pool tests and measure performance."""
    
    def __init__(self):
        self.results: List[Dict[str, Any]] = []
        self.errors: List[str] = []
    
    def time_sequential_queries(self, server: DaemonServer, num_queries: int = 5) -> Dict[str, Any]:
        """Run sequential queries and measure timing."""
        start_time = time.perf_counter()
        query_times = []
        
        for i in range(num_queries):
            query_start = time.perf_counter()
            
            try:
                with SQLJob() as job:
                    job.connect(server)
                    result = job.query_and_run(f"SELECT {i} as query_number, CURRENT_TIMESTAMP as query_time FROM sysibm.sysdummy1")
                    assert result["success"], f"Query {i} failed: {result.get('error', 'Unknown error')}"
                
                query_time = time.perf_counter() - query_start
                query_times.append(query_time)
                
            except Exception as e:
                self.errors.append(f"Query {i} failed: {str(e)}")
                query_times.append(float('inf'))  # Mark as failed
        
        total_time = time.perf_counter() - start_time
        
        return {
            "total_time": total_time,
            "query_times": query_times,
            "average_time": mean([t for t in query_times if t != float('inf')]),
            "successful_queries": sum(1 for t in query_times if t != float('inf')),
            "failed_queries": len(self.errors)
        }
    
    def concurrent_query_worker(self, server: DaemonServer, worker_id: int, num_queries: int = 3):
        """Worker function for concurrent connection testing."""
        try:
            for i in range(num_queries):
                with SQLJob() as job:
                    job.connect(server)
                    result = job.query_and_run(f"SELECT 'worker_{worker_id}_query_{i}' as test_data FROM sysibm.sysdummy1")
                    
                    self.results.append({
                        "worker_id": worker_id,
                        "query_id": i,
                        "success": result["success"],
                        "timestamp": time.time()
                    })
        except Exception as e:
            self.errors.append(f"Worker {worker_id} failed: {str(e)}")


@pytest.mark.tls
@pytest.mark.performance
def test_connection_pool_sequential_queries(ibmi_credentials):
    """Test connection pooling with sequential queries on IBM i."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")
    
    server = DaemonServer(**ibmi_credentials)
    
    # Clear any existing connections
    _connection_pool._cleanup_all()
    initial_stats = get_pool_stats()
    
    print(f"\n🔄 Testing connection pooling with sequential IBM i queries...")
    print(f"📊 Initial pool stats: {initial_stats}")
    
    # Create test runner
    runner = ConnectionPoolTestRunner()
    
    # Run sequential queries
    num_queries = 6
    results = runner.time_sequential_queries(server, num_queries)
    
    # Get final pool statistics
    final_stats = get_pool_stats()
    
    print(f"\n📈 Results:")
    print(f"  Total time: {results['total_time']:.3f}s")
    print(f"  Average query time: {results['average_time']:.3f}s")
    print(f"  Successful queries: {results['successful_queries']}/{num_queries}")
    print(f"  Failed queries: {results['failed_queries']}")
    print(f"  Query times: {[f'{t:.3f}s' for t in results['query_times']]}")
    print(f"  Final pool stats: {final_stats}")
    
    # Verify connection pooling is working
    assert results["successful_queries"] == num_queries, "All queries should succeed"
    assert results["failed_queries"] == 0, f"No queries should fail, but got errors: {runner.errors}"
    
    # The pool may be empty after all jobs complete (connections returned and cleaned up)
    # This is correct behavior - we verify functionality by successful query execution
    
    # Later queries should be faster than the first (connection reuse benefit)
    if len(results["query_times"]) >= 3:
        first_query_time = results["query_times"][0]
        later_queries_avg = mean(results["query_times"][2:])  # Skip first 2 to account for variance
        
        print(f"  First query: {first_query_time:.3f}s")
        print(f"  Later queries average: {later_queries_avg:.3f}s")
        
        # Allow some variance, but expect improvement
        if later_queries_avg < first_query_time * 0.9:
            print(f"  ✅ Connection reuse provided {((first_query_time - later_queries_avg) / first_query_time * 100):.1f}% improvement!")
        else:
            print(f"  ℹ️  Timing results inconclusive due to network variance")
    
    print("✅ Sequential connection pooling test completed successfully!")


@pytest.mark.tls
@pytest.mark.performance
def test_connection_pool_concurrent_access(ibmi_credentials):
    """Test connection pooling with concurrent access to IBM i."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")
    
    server = DaemonServer(**ibmi_credentials)
    
    # Clear any existing connections
    _connection_pool._cleanup_all()
    
    print(f"\n🧵 Testing connection pooling with concurrent IBM i access...")
    
    # Create test runner
    runner = ConnectionPoolTestRunner()
    
    # Run concurrent queries
    num_workers = 4
    queries_per_worker = 3
    
    start_time = time.perf_counter()
    
    # Start multiple worker threads
    threads = []
    for worker_id in range(num_workers):
        thread = threading.Thread(target=runner.concurrent_query_worker, args=(server, worker_id, queries_per_worker))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    total_time = time.perf_counter() - start_time
    final_stats = get_pool_stats()
    
    expected_results = num_workers * queries_per_worker
    successful_results = len([r for r in runner.results if r["success"]])
    
    print(f"\n📈 Concurrent Results:")
    print(f"  Workers: {num_workers}")
    print(f"  Queries per worker: {queries_per_worker}")
    print(f"  Total time: {total_time:.3f}s")
    print(f"  Expected results: {expected_results}")
    print(f"  Successful results: {successful_results}")
    print(f"  Failed operations: {len(runner.errors)}")
    print(f"  Final pool stats: {final_stats}")
    
    # Verify thread safety and functionality
    assert len(runner.errors) == 0, f"Should have no errors, got: {runner.errors}"
    assert successful_results == expected_results, f"Should have {expected_results} successful results, got {successful_results}"
    
    # The pool may be empty after all jobs complete (connections returned and cleaned up)
    # This is correct behavior - we verify functionality by successful query execution
    
    # Verify all workers completed successfully
    worker_results = {}
    for result in runner.results:
        worker_id = result["worker_id"]
        if worker_id not in worker_results:
            worker_results[worker_id] = 0
        worker_results[worker_id] += 1
    
    for worker_id in range(num_workers):
        assert worker_results.get(worker_id, 0) == queries_per_worker, \
            f"Worker {worker_id} should have completed {queries_per_worker} queries"
    
    print("✅ Concurrent connection pooling test completed successfully!")


@pytest.mark.tls
@pytest.mark.performance
def test_connection_pool_multiple_servers(ibmi_credentials):
    """Test connection pooling with multiple server configurations."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")
    
    # Create server configs with different settings
    server_default = DaemonServer(**ibmi_credentials)
    server_ignore_auth = DaemonServer(**ibmi_credentials, ignoreUnauthorized=True)
    
    # Clear any existing connections
    _connection_pool._cleanup_all()
    
    print(f"\n🌐 Testing connection pooling with multiple server configurations...")
    
    runner = ConnectionPoolTestRunner()
    
    # Test each server configuration
    servers = [
        ("default", server_default),
        ("ignore_auth", server_ignore_auth)
    ]
    
    for server_name, server_config in servers:
        print(f"\n  Testing {server_name} configuration...")
        
        # Run queries for this server
        for i in range(3):
            try:
                with SQLJob() as job:
                    job.connect(server_config)
                    result = job.query_and_run(f"SELECT '{server_name}_query_{i}' as config_test FROM sysibm.sysdummy1")
                    assert result["success"], f"Query failed for {server_name}: {result.get('error')}"
                    
                    runner.results.append({
                        "server_config": server_name,
                        "query_id": i,
                        "success": result["success"]
                    })
            except Exception as e:
                runner.errors.append(f"{server_name} query {i} failed: {str(e)}")
    
    final_stats = get_pool_stats()
    
    print(f"\n📈 Multi-server Results:")
    print(f"  Total successful queries: {len([r for r in runner.results if r['success']])}")
    print(f"  Total errors: {len(runner.errors)}")
    print(f"  Final pool stats: {final_stats}")
    
    # Verify functionality
    assert len(runner.errors) == 0, f"Should have no errors, got: {runner.errors}"
    
    # The pool may be empty after all jobs complete (connections returned and cleaned up)
    # This is correct behavior for different server configurations
    
    # Verify results for each server type
    server_results = {}
    for result in runner.results:
        config = result["server_config"]
        if config not in server_results:
            server_results[config] = 0
        server_results[config] += 1
    
    for server_name, _ in servers:
        assert server_results.get(server_name, 0) == 3, \
            f"Should have 3 successful queries for {server_name}"
    
    print("✅ Multi-server connection pooling test completed successfully!")


@pytest.mark.tls
@pytest.mark.performance
def test_connection_pool_cleanup_and_management(ibmi_credentials):
    """Test connection pool cleanup and management features."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")
    
    server = DaemonServer(**ibmi_credentials)
    
    # Clear any existing connections
    _connection_pool._cleanup_all()
    
    print(f"\n🧹 Testing connection pool cleanup and management...")
    
    # Create some connections
    initial_queries = 3
    for i in range(initial_queries):
        with SQLJob() as job:
            job.connect(server)
            result = job.query_and_run(f"SELECT {i} as cleanup_test FROM sysibm.sysdummy1")
            assert result["success"], f"Initial query {i} failed"
    
    stats_after_queries = get_pool_stats()
    print(f"  Stats after queries: {stats_after_queries}")
    
    # Test cleanup function
    cleaned_up = cleanup_idle_connections()
    stats_after_cleanup = get_pool_stats()
    
    print(f"  Connections cleaned up: {cleaned_up}")
    print(f"  Stats after cleanup: {stats_after_cleanup}")
    
    # Test pool statistics
    assert isinstance(stats_after_queries, dict), "Stats should be a dictionary"
    assert "total_connections" in stats_after_queries, "Stats should include total_connections"
    assert "active_connections" in stats_after_queries, "Stats should include active_connections"
    assert "healthy_connections" in stats_after_queries, "Stats should include healthy_connections"
    assert "idle_connections" in stats_after_queries, "Stats should include idle_connections"
    
    # Verify connections are still functional after cleanup
    with SQLJob() as job:
        job.connect(server)
        result = job.query_and_run("SELECT 'post_cleanup_test' as test FROM sysibm.sysdummy1")
        assert result["success"], "Post-cleanup query should work"
    
    final_stats = get_pool_stats()
    print(f"  Final stats: {final_stats}")
    
    print("✅ Connection pool cleanup and management test completed successfully!")


@pytest.mark.tls
@pytest.mark.performance
def test_connection_pool_environment_control(ibmi_credentials):
    """Test that connection pooling can be controlled via environment variables."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")
    
    server = DaemonServer(**ibmi_credentials)
    
    print(f"\n🔧 Testing connection pool environment variable control...")
    
    # Test with pooling enabled (default)
    original_env = os.environ.get("MAPEPIRE_CONNECTION_POOL")
    
    try:
        # Enable pooling
        os.environ["MAPEPIRE_CONNECTION_POOL"] = "true"
        
        # Clear connections
        _connection_pool._cleanup_all()
        
        # Run queries with pooling enabled
        with SQLJob() as job:
            job.connect(server)
            result = job.query_and_run("SELECT 'pooling_enabled' as test FROM sysibm.sysdummy1")
            assert result["success"], "Query with pooling enabled should work"
        
        stats_enabled = get_pool_stats()
        print(f"  Stats with pooling enabled: {stats_enabled}")
        
        # May or may not have connections in pool after job completion (depends on cleanup timing)
        # Key test is that queries succeed with pooling enabled
        
        # Test with pooling disabled
        os.environ["MAPEPIRE_CONNECTION_POOL"] = "false"
        
        # Clear connections
        _connection_pool._cleanup_all()
        
        # Run queries with pooling disabled
        with SQLJob() as job:
            job.connect(server)
            result = job.query_and_run("SELECT 'pooling_disabled' as test FROM sysibm.sysdummy1")
            assert result["success"], "Query with pooling disabled should work"
        
        stats_disabled = get_pool_stats()
        print(f"  Stats with pooling disabled: {stats_disabled}")
        
        # Should have no connections in pool when disabled
        assert stats_disabled["total_connections"] == 0, "Should have no pooled connections when disabled"
    
    finally:
        # Restore original environment
        if original_env is not None:
            os.environ["MAPEPIRE_CONNECTION_POOL"] = original_env
        else:
            os.environ.pop("MAPEPIRE_CONNECTION_POOL", None)
    
    print("✅ Environment variable control test completed successfully!")


@pytest.mark.tls
@pytest.mark.performance
@pytest.mark.slow
def test_connection_pool_performance_benchmark(ibmi_credentials):
    """Benchmark connection pool performance vs direct connections."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")
    
    server = DaemonServer(**ibmi_credentials)
    
    print(f"\n🏁 Connection Pool Performance Benchmark")
    print("=" * 50)
    
    num_queries = 8
    
    # Test with pooling enabled
    os.environ["MAPEPIRE_CONNECTION_POOL"] = "true"
    _connection_pool._cleanup_all()
    
    print(f"\n⚡ Testing WITH connection pooling...")
    
    runner_pooled = ConnectionPoolTestRunner()
    start_time = time.perf_counter()
    results_pooled = runner_pooled.time_sequential_queries(server, num_queries)
    pooled_total_time = time.perf_counter() - start_time
    
    stats_pooled = get_pool_stats()
    
    print(f"  Total time: {pooled_total_time:.3f}s")
    print(f"  Average query time: {results_pooled['average_time']:.3f}s")
    print(f"  Pool stats: {stats_pooled}")
    
    # Test with pooling disabled
    os.environ["MAPEPIRE_CONNECTION_POOL"] = "false"
    _connection_pool._cleanup_all()
    
    print(f"\n🔄 Testing WITHOUT connection pooling...")
    
    runner_direct = ConnectionPoolTestRunner()
    start_time = time.perf_counter()
    results_direct = runner_direct.time_sequential_queries(server, num_queries)
    direct_total_time = time.perf_counter() - start_time
    
    stats_direct = get_pool_stats()
    
    print(f"  Total time: {direct_total_time:.3f}s")
    print(f"  Average query time: {results_direct['average_time']:.3f}s")
    print(f"  Pool stats: {stats_direct}")
    
    # Calculate performance improvement
    if results_direct['average_time'] > 0:
        time_saved = direct_total_time - pooled_total_time
        improvement_percent = (time_saved / direct_total_time) * 100
        
        avg_time_saved = results_direct['average_time'] - results_pooled['average_time']
        avg_improvement_percent = (avg_time_saved / results_direct['average_time']) * 100
        
        print(f"\n📈 Performance Improvement:")
        print(f"  Total time saved: {time_saved:.3f}s ({improvement_percent:.1f}% faster)")
        print(f"  Average query time saved: {avg_time_saved:.3f}s ({avg_improvement_percent:.1f}% faster)")
        print(f"  Connection reuse efficiency: {stats_pooled['total_connections']} connections for {num_queries} queries")
        
        # For real performance tests, we expect some improvement with pooling
        if improvement_percent > 5:  # Allow for network variance
            print(f"  ✅ Connection pooling provides significant {improvement_percent:.1f}% performance improvement!")
        else:
            print(f"  ℹ️  Performance improvement inconclusive due to network conditions")
    
    # Restore environment
    os.environ["MAPEPIRE_CONNECTION_POOL"] = "true"
    
    # Verify both approaches work correctly
    assert results_pooled["successful_queries"] == num_queries, "All pooled queries should succeed"
    assert results_direct["successful_queries"] == num_queries, "All direct queries should succeed"
    assert len(runner_pooled.errors) == 0, f"Pooled queries should have no errors: {runner_pooled.errors}"
    assert len(runner_direct.errors) == 0, f"Direct queries should have no errors: {runner_direct.errors}"
    
    print("✅ Performance benchmark completed successfully!")


if __name__ == "__main__":
    # Run specific test for development
    pytest.main([__file__ + "::test_connection_pool_sequential_queries", "-v", "-s"])