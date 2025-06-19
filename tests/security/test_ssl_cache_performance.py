"""
Performance tests for SSL context caching with real IBM i connections.

These tests verify that SSL caching provides measurable performance improvements
when establishing multiple connections to IBM i systems.
"""

import time
from statistics import mean, stdev
from typing import Any, Dict, List

import pytest

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import DaemonServer
from mapepire_python.ssl_cache import (
    clear_certificate_cache,
    clear_ssl_cache,
    get_certificate_cache_stats,
    get_ssl_cache_stats,
)


class SSLConnectionTimer:
    """Helper class to measure SSL connection times."""

    def __init__(self):
        self.times: List[float] = []

    def time_connection(self, server: DaemonServer) -> Dict[str, Any]:
        """Time a single SSL connection and return results."""
        job = SQLJob()

        start_time = time.perf_counter()
        try:
            result = job.connect(server)
            connect_time = time.perf_counter() - start_time

            if result.get("success"):
                # Test a simple query to ensure connection works
                query_start = time.perf_counter()
                query_result = job.query_and_run("SELECT 1 from sysibm.sysdummy1")
                query_time = time.perf_counter() - query_start

                total_time = time.perf_counter() - start_time
                self.times.append(total_time)

                return {
                    "success": True,
                    "connect_time": connect_time,
                    "query_time": query_time,
                    "total_time": total_time,
                    "query_success": query_result.get("success", False),
                }
            else:
                return {"success": False, "error": result.get("error", "Unknown error")}

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            job.close()

    def get_statistics(self) -> Dict[str, float]:
        """Get timing statistics."""
        if not self.times:
            return {"count": 0, "mean": 0.0, "std": 0.0, "min": 0.0, "max": 0.0}

        return {
            "count": len(self.times),
            "mean": mean(self.times),
            "std": stdev(self.times) if len(self.times) > 1 else 0.0,
            "min": min(self.times),
            "max": max(self.times),
        }


@pytest.mark.tls
@pytest.mark.performance
def test_ssl_cache_performance_improvement(ibmi_credentials):
    """Test that SSL caching improves connection performance."""
    # Skip if credentials not available
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")

    # Create server config with caching enabled
    server_cached = DaemonServer(**ibmi_credentials, ssl_cache_enabled=True)
    server_uncached = DaemonServer(**ibmi_credentials, ssl_cache_enabled=False)

    # Test without caching (baseline)
    clear_ssl_cache()
    clear_certificate_cache()
    uncached_timer = SSLConnectionTimer()

    print(f"\n🔄 Testing {3} connections WITHOUT caching...")
    for i in range(3):
        result = uncached_timer.time_connection(server_uncached)
        if not result["success"]:
            pytest.fail(f"Connection {i+1} failed without caching: {result.get('error')}")
        print(f"  Connection {i+1}: {result['total_time']:.3f}s")

    uncached_stats = uncached_timer.get_statistics()

    # Test with caching enabledSELECT 1 as test_value
    clear_ssl_cache()
    clear_certificate_cache()
    cached_timer = SSLConnectionTimer()

    print(f"\n⚡ Testing {5} connections WITH caching...")
    for i in range(5):
        result = cached_timer.time_connection(server_cached)
        if not result["success"]:
            pytest.fail(f"Connection {i+1} failed with caching: {result.get('error')}")
        print(f"  Connection {i+1}: {result['total_time']:.3f}s")

    cached_stats = cached_timer.get_statistics()

    # Get cache statistics
    ssl_cache_stats = get_ssl_cache_stats()
    cert_cache_stats = get_certificate_cache_stats()

    # Print detailed results
    print(f"\n📊 Performance Results:")
    print(f"  Without caching: {uncached_stats['mean']:.3f}s ± {uncached_stats['std']:.3f}s")
    print(f"  With caching:    {cached_stats['mean']:.3f}s ± {cached_stats['std']:.3f}s")
    print(
        f"  Speed improvement: {((uncached_stats['mean'] - cached_stats['mean']) / uncached_stats['mean'] * 100):.1f}%"
    )

    print(f"\n📈 Cache Statistics:")
    print(f"  SSL Context Cache: {ssl_cache_stats}")
    print(f"  Certificate Cache: {cert_cache_stats}")

    # Verify caching worked
    assert ssl_cache_stats["hits"] >= 4, "Should have at least 4 SSL context cache hits"
    assert ssl_cache_stats["size"] >= 1, "Should have cached at least 1 SSL context"
    assert cert_cache_stats["size"] >= 1, "Should have cached at least 1 certificate"

    # Verify performance improvement
    # Note: Due to network variability, we check for any improvement rather than a strict threshold
    improvement_seconds = uncached_stats["mean"] - cached_stats["mean"]
    improvement_percent = (improvement_seconds / uncached_stats["mean"]) * 100

    print(f"\n✅ Performance improvement: {improvement_seconds:.3f}s ({improvement_percent:.1f}%)")

    # For real performance improvement, we expect at least some benefit from the 2nd connection onwards
    # The first connection will still be slow due to cache miss
    if cached_stats["count"] >= 2:
        # Check that later connections are faster than the average uncached time
        later_connections = cached_timer.times[1:]  # Skip first connection (cache miss)
        avg_later_time = mean(later_connections)
        print(
            f"  Average time for cached connections (2-{len(cached_timer.times)}): {avg_later_time:.3f}s"
        )

        # Later connections should be faster than uncached baseline
        assert (
            avg_later_time < uncached_stats["mean"]
        ), f"Cached connections ({avg_later_time:.3f}s) should be faster than uncached ({uncached_stats['mean']:.3f}s)"


@pytest.mark.tls
@pytest.mark.performance
def test_ssl_cache_with_different_servers(ibmi_credentials):
    """Test SSL caching behavior with different server configurations."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")

    clear_ssl_cache()
    clear_certificate_cache()

    # Create different server configurations
    server1 = DaemonServer(**ibmi_credentials, ssl_cache_enabled=True, ignoreUnauthorized=True)
    server2 = DaemonServer(**ibmi_credentials, ssl_cache_enabled=True, ignoreUnauthorized=False)

    timer = SSLConnectionTimer()

    print(f"\n🔄 Testing caching with different SSL configurations...")

    # Connect with server1 (ignoreUnauthorized=True)
    result1 = timer.time_connection(server1)
    assert result1["success"], f"Server1 connection failed: {result1.get('error')}"
    print(f"  Server1 (ignoreUnauthorized=True): {result1['total_time']:.3f}s")

    # Connect with server2 (ignoreUnauthorized=False)
    result2 = timer.time_connection(server2)
    assert result2["success"], f"Server2 connection failed: {result2.get('error')}"
    print(f"  Server2 (ignoreUnauthorized=False): {result2['total_time']:.3f}s")

    # Connect again with server1 - should hit cache
    result3 = timer.time_connection(server1)
    assert result3["success"], f"Server1 second connection failed: {result3.get('error')}"
    print(f"  Server1 (cached): {result3['total_time']:.3f}s")

    # Connect again with server2 - should hit cache
    result4 = timer.time_connection(server2)
    assert result4["success"], f"Server2 second connection failed: {result4.get('error')}"
    print(f"  Server2 (cached): {result4['total_time']:.3f}s")

    ssl_cache_stats = get_ssl_cache_stats()
    print(f"\n📈 Cache Statistics: {ssl_cache_stats}")

    # Should have 2 different SSL contexts cached (different ignoreUnauthorized settings)
    assert ssl_cache_stats["size"] == 2, "Should cache 2 different SSL contexts"
    assert ssl_cache_stats["hits"] == 2, "Should have 2 cache hits"
    assert ssl_cache_stats["misses"] == 2, "Should have 2 cache misses"


@pytest.mark.tls
@pytest.mark.performance
@pytest.mark.slow
def test_ssl_cache_memory_efficiency(ibmi_credentials):
    """Test that SSL cache doesn't grow unbounded and handles cleanup properly."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")

    clear_ssl_cache()
    clear_certificate_cache()

    server = DaemonServer(**ibmi_credentials, ssl_cache_enabled=True)

    print(f"\n🧠 Testing SSL cache memory efficiency with multiple connections...")

    # Make several connections to verify cache size stays reasonable
    timer = SSLConnectionTimer()
    for i in range(10):
        result = timer.time_connection(server)
        assert result["success"], f"Connection {i+1} failed: {result.get('error')}"

        if i % 3 == 0:  # Print every 3rd connection
            stats = get_ssl_cache_stats()
            print(f"  Connection {i+1}: {result['total_time']:.3f}s, Cache size: {stats['size']}")

    final_stats = get_ssl_cache_stats()
    cert_stats = get_certificate_cache_stats()

    print(f"\n📊 Final Statistics:")
    print(f"  SSL Cache: {final_stats}")
    print(f"  Certificate Cache: {cert_stats}")
    print(f"  Total connections: {timer.get_statistics()['count']}")
    print(f"  Average connection time: {timer.get_statistics()['mean']:.3f}s")

    # Cache should not grow excessively for identical connections
    assert final_stats["size"] <= 2, "SSL cache should not grow for identical connections"
    assert cert_stats["size"] <= 2, "Certificate cache should not grow for identical connections"

    # Should have high cache hit rate after first connection
    hit_rate = final_stats["hits"] / (final_stats["hits"] + final_stats["misses"])
    print(f"  Cache hit rate: {hit_rate:.1%}")
    assert hit_rate >= 0.8, f"Cache hit rate should be >= 80%, got {hit_rate:.1%}"


@pytest.mark.tls
@pytest.mark.performance
def test_certificate_caching_performance(ibmi_credentials):
    """Test certificate caching specifically."""
    if not all(ibmi_credentials.values()):
        pytest.skip("IBM i credentials not configured")

    from mapepire_python.ssl import get_certificate

    clear_certificate_cache()

    server = DaemonServer(**ibmi_credentials, ssl_cache_enabled=True)

    print(f"\n🔐 Testing certificate caching performance...")

    cert_times = []

    # Time certificate retrieval without cache
    start_time = time.perf_counter()
    cert1 = get_certificate(server)
    first_time = time.perf_counter() - start_time
    cert_times.append(first_time)

    assert cert1 is not None, "Should retrieve certificate"
    print(f"  First certificate fetch: {first_time:.3f}s")

    # Time certificate retrieval with cache
    for i in range(3):
        start_time = time.perf_counter()
        cert2 = get_certificate(server)
        cached_time = time.perf_counter() - start_time
        cert_times.append(cached_time)

        assert cert2 == cert1, "Cached certificate should match original"
        print(f"  Cached certificate fetch {i+1}: {cached_time:.3f}s")

    cert_cache_stats = get_certificate_cache_stats()
    print(f"\n📈 Certificate Cache Statistics: {cert_cache_stats}")

    # Verify caching worked
    assert cert_cache_stats["hits"] == 3, "Should have 3 certificate cache hits"
    assert cert_cache_stats["misses"] == 1, "Should have 1 certificate cache miss"
    assert cert_cache_stats["size"] == 1, "Should have 1 cached certificate"

    # Cached retrievals should be significantly faster
    avg_cached_time = mean(cert_times[1:])
    improvement = (first_time - avg_cached_time) / first_time * 100
    print(f"  Certificate caching improvement: {improvement:.1f}%")

    # Certificate cache should provide substantial improvement
    assert avg_cached_time < first_time, "Cached certificate retrieval should be faster"


if __name__ == "__main__":
    # Allow running tests directly for development
    pytest.main([__file__ + "::test_ssl_cache_performance_improvement", "-v", "-s"])
