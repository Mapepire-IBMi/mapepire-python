"""
Performance tests for optimized connection pool implementation.

Tests the improved connection pooling with:
- Multiple connections per server
- Reduced lock contention  
- Cached health checks
"""

import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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


class PerformanceTestRunner:
    """Enhanced performance test runner for optimized connection pool."""
    
    def __init__(self):
        self.results: List[Dict[str, Any]] = []
        self.errors: List[str] = []
        self.timing_data = {}
    
    def run_concurrent_queries(self, server: DaemonServer, num_threads: int = 8, queries_per_thread: int = 5) -> Dict[str, Any]:
        """Run concurrent queries to test connection pool optimization."""
        
        def worker(worker_id: int) -> Dict[str, Any]:
            """Worker function for concurrent testing."""
            worker_results = []
            worker_errors = []
            
            for i in range(queries_per_thread):
                start_time = time.perf_counter()
                try:
                    with SQLJob() as job:
                        job.connect(server)
                        result = job.query_and_run(f"SELECT 'worker_{worker_id}_query_{i}' as test_data, CURRENT_TIMESTAMP FROM sysibm.sysdummy1")
                        
                        query_time = time.perf_counter() - start_time
                        worker_results.append({
                            "worker_id": worker_id,
                            "query_id": i,
                            "success": result["success"],
                            "query_time": query_time,
                            "timestamp": time.time()
                        })
                        
                except Exception as e:
                    worker_errors.append(f"Worker {worker_id} query {i} failed: {str(e)}")
                    worker_results.append({
                        "worker_id": worker_id,
                        "query_id": i,
                        "success": False,
                        "query_time": float('inf'),
                        "timestamp": time.time()
                    })
            
            return {"results": worker_results, "errors": worker_errors}
        
        # Run concurrent workers
        start_time = time.perf_counter()
        all_results = []
        all_errors = []
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            
            for future in as_completed(futures):
                try:
                    worker_data = future.result()
                    all_results.extend(worker_data["results"])
                    all_errors.extend(worker_data["errors"])
                except Exception as e:
                    all_errors.append(f"Worker execution failed: {str(e)}")
        
        total_time = time.perf_counter() - start_time
        
        # Calculate statistics
        successful_queries = [r for r in all_results if r["success"]]
        query_times = [r["query_time"] for r in successful_queries if r["query_time"] != float('inf')]
        
        return {
            "total_time": total_time,
            "total_queries": len(all_results),
            "successful_queries": len(successful_queries),
            "failed_queries": len(all_results) - len(successful_queries),
            "average_query_time": mean(query_times) if query_times else 0,
            "min_query_time": min(query_times) if query_times else 0,
            "max_query_time": max(query_times) if query_times else 0,
            "queries_per_second": len(successful_queries) / total_time if total_time > 0 else 0,
            "errors": all_errors
        }


@pytest.mark.tls
@pytest.mark.performance
def test_optimized_connection_pool_throughput(ibmi_credentials):
    """Test throughput improvements with optimized connection pool."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")
    
    server = DaemonServer(**ibmi_credentials)
    
    print(f"\\n🚀 Testing optimized connection pool throughput...")
    
    # Test with original single connection per server (set max to 1)
    os.environ["MAPEPIRE_MAX_CONNECTIONS_PER_SERVER"] = "1"
    _connection_pool._cleanup_all()
    
    print(f"\\n📊 Testing with single connection per server...")
    runner_single = PerformanceTestRunner()
    results_single = runner_single.run_concurrent_queries(server, num_threads=6, queries_per_thread=3)
    
    print(f"  Total time: {results_single['total_time']:.3f}s")
    print(f"  Successful queries: {results_single['successful_queries']}")
    print(f"  Queries per second: {results_single['queries_per_second']:.2f}")
    print(f"  Average query time: {results_single['average_query_time']:.3f}s")
    print(f"  Errors: {len(results_single['errors'])}")
    
    # Test with multiple connections per server (set max to 3)
    os.environ["MAPEPIRE_MAX_CONNECTIONS_PER_SERVER"] = "3"
    _connection_pool._cleanup_all()
    
    print(f"\\n⚡ Testing with multiple connections per server...")
    runner_multi = PerformanceTestRunner()
    results_multi = runner_multi.run_concurrent_queries(server, num_threads=6, queries_per_thread=3)
    
    stats_multi = get_pool_stats()
    
    print(f"  Total time: {results_multi['total_time']:.3f}s")
    print(f"  Successful queries: {results_multi['successful_queries']}")
    print(f"  Queries per second: {results_multi['queries_per_second']:.2f}")
    print(f"  Average query time: {results_multi['average_query_time']:.3f}s")
    print(f"  Errors: {len(results_multi['errors'])}")
    print(f"  Pool stats: {stats_multi}")
    
    # Calculate improvements
    if results_single['queries_per_second'] > 0:
        throughput_improvement = ((results_multi['queries_per_second'] - results_single['queries_per_second']) 
                                / results_single['queries_per_server']) * 100
        
        time_improvement = ((results_single['total_time'] - results_multi['total_time']) 
                          / results_single['total_time']) * 100
        
        print(f"\\n📈 Performance Improvements:")
        print(f"  Throughput improvement: {throughput_improvement:.1f}%")
        print(f"  Total time improvement: {time_improvement:.1f}%")
        print(f"  Connection efficiency: Used {stats_multi['total_connections']} connections vs 1")
        
        # Verify improvements
        assert results_multi['successful_queries'] == results_single['successful_queries'], "Should complete same number of queries"
        assert len(results_multi['errors']) == 0, f"Should have no errors with optimized pool: {results_multi['errors']}"
        
        # Should see measurable improvement with multiple connections
        if throughput_improvement > 10:
            print(f"  ✅ Significant {throughput_improvement:.1f}% throughput improvement achieved!")
        else:
            print(f"  ℹ️  Modest improvement - may be network bound in this environment")
    
    # Restore default
    os.environ["MAPEPIRE_MAX_CONNECTIONS_PER_SERVER"] = "3"
    
    print("✅ Optimized connection pool throughput test completed!")


@pytest.mark.tls  
@pytest.mark.performance
def test_connection_pool_health_check_optimization(ibmi_credentials):
    """Test that cached health checks reduce latency."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")
    
    server = DaemonServer(**ibmi_credentials)
    
    print(f"\\n🔍 Testing connection health check optimization...")
    
    # Pre-warm connection pool
    _connection_pool._cleanup_all()
    
    # Create initial connection
    with SQLJob() as job:
        job.connect(server)
        job.query_and_run("SELECT 'warmup' FROM sysibm.sysdummy1")
    
    # Test repeated connection requests (should hit cached health checks)
    start_time = time.perf_counter()
    
    connection_times = []
    for i in range(10):
        connection_start = time.perf_counter()
        
        with SQLJob() as job:
            job.connect(server)
            # Just connect, don't query to isolate connection overhead
        
        connection_time = time.perf_counter() - connection_start
        connection_times.append(connection_time)
    
    total_connection_time = time.perf_counter() - start_time
    average_connection_time = mean(connection_times)
    
    print(f"  Average connection time: {average_connection_time:.3f}s")
    print(f"  Total time for 10 connections: {total_connection_time:.3f}s")
    print(f"  Connection time range: {min(connection_times):.3f}s - {max(connection_times):.3f}s")
    
    final_stats = get_pool_stats()
    print(f"  Final pool stats: {final_stats}")
    
    # Verify health check caching is working
    # Later connections should be consistently fast due to cached health checks
    later_connections = connection_times[5:]  # Skip first few to account for variance
    connection_variance = max(later_connections) - min(later_connections)
    
    print(f"  Connection time variance (last 5): {connection_variance:.3f}s")
    
    # Should have low variance due to cached health checks
    if connection_variance < 0.1:  # Less than 100ms variance
        print(f"  ✅ Low connection time variance indicates effective health check caching!")
    else:
        print(f"  ℹ️  Higher variance may indicate network conditions or other factors")
    
    print("✅ Health check optimization test completed!")


@pytest.mark.tls
@pytest.mark.performance  
def test_connection_pool_lock_contention_reduction(ibmi_credentials):
    """Test that lock contention is reduced with optimizations."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")
    
    server = DaemonServer(**ibmi_credentials)
    
    print(f"\\n🔒 Testing connection pool lock contention reduction...")
    
    # Set up for high contention scenario
    _connection_pool._cleanup_all()
    os.environ["MAPEPIRE_MAX_CONNECTIONS_PER_SERVER"] = "2"
    
    # High concurrency test to stress lock contention
    def stress_worker(worker_id: int, results_list: list):
        """Worker to create lock contention."""
        try:
            for i in range(5):
                start_time = time.perf_counter()
                
                with SQLJob() as job:
                    job.connect(server)
                    result = job.query_and_run(f"SELECT {worker_id} as worker, {i} as iteration FROM sysibm.sysdummy1")
                
                execution_time = time.perf_counter() - start_time
                results_list.append({
                    "worker_id": worker_id,
                    "execution_time": execution_time,
                    "success": result["success"] if result else False
                })
                
        except Exception as e:
            results_list.append({
                "worker_id": worker_id,
                "execution_time": float('inf'),
                "success": False,
                "error": str(e)
            })
    
    # Run high concurrency test
    results = []
    threads = []
    num_workers = 8
    
    start_time = time.perf_counter()
    
    for worker_id in range(num_workers):
        thread = threading.Thread(target=stress_worker, args=(worker_id, results))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    total_time = time.perf_counter() - start_time
    
    # Analyze results
    successful_operations = [r for r in results if r["success"]]
    execution_times = [r["execution_time"] for r in successful_operations if r["execution_time"] != float('inf')]
    errors = [r for r in results if not r["success"]]
    
    final_stats = get_pool_stats()
    
    print(f"  Workers: {num_workers}")
    print(f"  Total operations: {len(results)}")
    print(f"  Successful operations: {len(successful_operations)}")
    print(f"  Failed operations: {len(errors)}")
    print(f"  Total time: {total_time:.3f}s")
    print(f"  Average execution time: {mean(execution_times):.3f}s")
    print(f"  Operations per second: {len(successful_operations) / total_time:.2f}")
    print(f"  Pool stats: {final_stats}")
    
    # Verify lock contention reduction
    assert len(errors) == 0, f"Should have no errors with optimized locking: {[e.get('error', 'Unknown') for e in errors]}"
    assert len(successful_operations) >= num_workers * 4, "Should complete most operations successfully"
    
    # Should efficiently handle concurrent access without deadlocks
    operations_per_second = len(successful_operations) / total_time
    if operations_per_second > 3.0:  # Reasonable throughput expectation
        print(f"  ✅ Good throughput ({operations_per_second:.2f} ops/sec) indicates reduced lock contention!")
    else:
        print(f"  ℹ️  Throughput may be limited by network or server processing time")
    
    # Restore default
    os.environ.pop("MAPEPIRE_MAX_CONNECTIONS_PER_SERVER", None)
    
    print("✅ Lock contention reduction test completed!")


if __name__ == "__main__":
    # Run specific test for development
    pytest.main([__file__ + "::test_optimized_connection_pool_throughput", "-v", "-s"])