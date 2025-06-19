"""
Test SSL context caching functionality.

These tests verify that SSL contexts are properly cached and reused
to improve connection performance while maintaining security.
"""

import os
import ssl
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from mapepire_python.data_types import DaemonServer
from mapepire_python.ssl_cache import (
    GenericCache,
    CachedItem,
    _get_ssl_context_key,
    _get_certificate_key,
    should_use_cache,
    get_cached_ssl_context,
    cache_ssl_context,
    get_cached_certificate,
    cache_certificate,
    clear_ssl_cache,
    clear_certificate_cache,
    get_ssl_cache_stats,
    get_certificate_cache_stats,
    # Backward compatibility aliases
    SSLCacheConfig,
    CachedSSLContext,
    CachedCertificate,
)


class TestGenericCache:
    """Test the generic cache implementation that underlies both SSL and certificate caching."""

    def setup_method(self):
        """Set up test environment."""
        self.cache = GenericCache[str](max_size=3, ttl_seconds=3600)

    def test_cache_miss_and_store(self):
        """Should handle cache miss and store new items."""
        # Cache miss
        result = self.cache.get("key1")
        assert result is None

        # Store item
        self.cache.put("key1", "value1")

        # Cache hit
        result = self.cache.get("key1")
        assert result is not None
        assert result.item == "value1"
        assert result.access_count == 1

    def test_cache_hit_updates_access(self):
        """Should update access count and time on cache hit."""
        self.cache.put("key1", "value1")

        # First access
        result1 = self.cache.get("key1")
        assert result1.access_count == 1
        first_access_time = result1.last_accessed

        # Small delay
        time.sleep(0.01)

        # Second access
        result2 = self.cache.get("key1")
        assert result2.access_count == 2
        assert result2.last_accessed > first_access_time

    def test_lru_eviction(self):
        """Should evict least recently used items when at capacity."""
        # Fill cache to capacity
        self.cache.put("key1", "value1")
        self.cache.put("key2", "value2")
        self.cache.put("key3", "value3")
        assert self.cache.get_stats()["size"] == 3

        # Access key1 to make it recently used
        self.cache.get("key1")

        # Add new item - should evict key2 (least recently used)
        self.cache.put("key4", "value4")
        
        assert self.cache.get("key1") is not None  # Still present
        assert self.cache.get("key2") is None      # Evicted
        assert self.cache.get("key3") is not None  # Still present
        assert self.cache.get("key4") is not None  # New item

    def test_expiration(self):
        """Should remove expired items."""
        cache = GenericCache[str](ttl_seconds=0.1)
        cache.put("key1", "value1")
        
        assert cache.get("key1") is not None
        
        # Wait for expiration
        time.sleep(0.2)
        
        # Should return None for expired item
        assert cache.get("key1") is None
        assert cache.get_stats()["size"] == 0

    def test_thread_safety(self):
        """Should be thread-safe."""
        results = []
        errors = []

        def cache_operations():
            try:
                for i in range(50):
                    key = f"key{i % 5}"
                    self.cache.put(key, f"value{i}")
                    result = self.cache.get(key)
                    results.append(result is not None)
            except Exception as e:
                errors.append(e)

        # Run operations in multiple threads
        threads = [threading.Thread(target=cache_operations) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(errors) == 0
        assert all(results)

    def test_clear_cache(self):
        """Should clear all cached items."""
        self.cache.put("key1", "value1")
        self.cache.put("key2", "value2")
        assert self.cache.get_stats()["size"] == 2

        self.cache.clear()
        assert self.cache.get_stats()["size"] == 0
        assert self.cache.get("key1") is None

    def test_statistics(self):
        """Should track cache statistics correctly."""
        # Initial stats
        stats = self.cache.get_stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["size"] == 0

        # Cache miss
        self.cache.get("key1")
        stats = self.cache.get_stats()
        assert stats["misses"] == 1

        # Cache store and hit
        self.cache.put("key1", "value1")
        self.cache.get("key1")
        stats = self.cache.get_stats()
        assert stats["hits"] == 1
        assert stats["size"] == 1


class TestCacheKeyGeneration:
    """Test cache key generation for SSL contexts and certificates."""

    def test_ssl_context_key_excludes_credentials(self):
        """SSL context keys should not include username/password."""
        server1 = DaemonServer(host="example.com", user="user1", password="pass1", port=8443)
        server2 = DaemonServer(host="example.com", user="user2", password="pass2", port=8443)

        key1 = _get_ssl_context_key(server1)
        key2 = _get_ssl_context_key(server2)

        assert key1 == key2
        assert "example.com:8443" in key1
        assert "user" not in key1
        assert "pass" not in key1

    def test_ssl_context_key_includes_ssl_config(self):
        """SSL context keys should include SSL-relevant configuration."""
        server1 = DaemonServer(host="example.com", user="u", password="p", port=8443, ignoreUnauthorized=True)
        server2 = DaemonServer(host="example.com", user="u", password="p", port=8443, ignoreUnauthorized=False)

        key1 = _get_ssl_context_key(server1)
        key2 = _get_ssl_context_key(server2)

        assert key1 != key2

    def test_ssl_context_key_includes_ca(self):
        """SSL context keys should include CA certificate hash."""
        ca_cert = "-----BEGIN CERTIFICATE-----\nMIIC..."
        server1 = DaemonServer(host="example.com", user="u", password="p", port=8443, ca=ca_cert)
        server2 = DaemonServer(host="example.com", user="u", password="p", port=8443, ca=None)

        key1 = _get_ssl_context_key(server1)
        key2 = _get_ssl_context_key(server2)

        assert key1 != key2

    def test_certificate_key_simple(self):
        """Certificate keys should only depend on host and port."""
        server1 = DaemonServer(host="example.com", user="user1", password="pass1", port=8443)
        server2 = DaemonServer(host="example.com", user="user2", password="pass2", port=8443)

        key1 = _get_certificate_key(server1)
        key2 = _get_certificate_key(server2)

        assert key1 == key2
        assert "cert:example.com:8443" == key1


class TestCacheEnabling:
    """Test cache enabling/disabling logic."""

    def test_should_use_cache_default_false(self):
        """Should default to cache disabled."""
        server = DaemonServer(host="example.com", user="user1", password="pass1", port=8443)
        assert should_use_cache(server) is False

    def test_should_use_cache_server_override(self):
        """Should respect server-level cache setting."""
        server_enabled = DaemonServer(host="example.com", user="u", password="p", port=8443, ssl_cache_enabled=True)
        server_disabled = DaemonServer(host="example.com", user="u", password="p", port=8443, ssl_cache_enabled=False)

        assert should_use_cache(server_enabled) is True
        assert should_use_cache(server_disabled) is False

    @patch.dict(os.environ, {"MAPEPIRE_SSL_CACHE": "true"})
    def test_should_use_cache_env_var_enabled(self):
        """Should respect environment variable when server setting is None."""
        server = DaemonServer(host="example.com", user="u", password="p", port=8443, ssl_cache_enabled=None)
        assert should_use_cache(server) is True

    @patch.dict(os.environ, {"MAPEPIRE_SSL_CACHE": "true"})
    def test_server_setting_overrides_env_var(self):
        """Server setting should override environment variable."""
        server = DaemonServer(host="example.com", user="u", password="p", port=8443, ssl_cache_enabled=False)
        assert should_use_cache(server) is False


class TestSSLContextCaching:
    """Test SSL context caching public API."""

    def setup_method(self):
        """Set up test environment."""
        self.server = DaemonServer(host="example.com", user="u", password="p", port=8443, ssl_cache_enabled=True)
        clear_ssl_cache()

    def test_ssl_context_caching_disabled(self):
        """Should return None when caching is disabled."""
        server = DaemonServer(host="example.com", user="u", password="p", port=8443, ssl_cache_enabled=False)
        
        result = get_cached_ssl_context(server)
        assert result is None

    def test_ssl_context_caching_enabled(self):
        """Should cache and retrieve SSL contexts when enabled."""
        mock_context = MagicMock(spec=ssl.SSLContext)
        
        # Should miss initially
        result = get_cached_ssl_context(self.server)
        assert result is None
        
        # Cache the context
        cache_ssl_context(self.server, mock_context)
        
        # Should hit now
        result = get_cached_ssl_context(self.server)
        assert result is mock_context
        
        # Stats should reflect usage
        stats = get_ssl_cache_stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["size"] == 1


class TestCertificateCaching:
    """Test certificate caching public API."""

    def setup_method(self):
        """Set up test environment."""
        self.server = DaemonServer(host="example.com", user="u", password="p", port=8443, ssl_cache_enabled=True)
        clear_certificate_cache()

    def test_certificate_caching_disabled(self):
        """Should return None when caching is disabled."""
        server = DaemonServer(host="example.com", user="u", password="p", port=8443, ssl_cache_enabled=False)
        
        result = get_cached_certificate(server)
        assert result is None

    def test_certificate_caching_enabled(self):
        """Should cache and retrieve certificates when enabled."""
        test_cert = b"-----BEGIN CERTIFICATE-----\nMIIC..."
        
        # Should miss initially
        result = get_cached_certificate(self.server)
        assert result is None
        
        # Cache the certificate
        cache_certificate(self.server, test_cert)
        
        # Should hit now
        result = get_cached_certificate(self.server)
        assert result == test_cert
        
        # Stats should reflect usage
        stats = get_certificate_cache_stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["size"] == 1


class TestIntegrationWithBaseConnection:
    """Test integration with BaseConnection SSL context creation."""
    
    def test_baseconnection_uses_cache(self):
        """BaseConnection should use SSL cache when enabled."""
        from mapepire_python.websocket import BaseConnection
        
        # Clear cache to start fresh
        clear_ssl_cache()
        
        server = DaemonServer(
            host="test.example.com",
            user="user1",
            password="pass1",
            port=8443,
            ignoreUnauthorized=True,  # Simple mode to avoid certificate fetching
            ssl_cache_enabled=True
        )
        
        # Create first connection - should miss cache
        conn1 = BaseConnection(server)
        ctx1 = conn1._create_ssl_context(server)
        stats_after_first = get_ssl_cache_stats()
        
        # Create second connection - should hit cache
        conn2 = BaseConnection(server)
        ctx2 = conn2._create_ssl_context(server)
        stats_after_second = get_ssl_cache_stats()
        
        # Verify caching behavior
        assert stats_after_first["hits"] == 0
        assert stats_after_first["misses"] == 1
        assert stats_after_first["size"] == 1
        
        assert stats_after_second["hits"] == 1
        assert stats_after_second["misses"] == 1
        assert stats_after_second["size"] == 1
        
        # Both contexts should be the same object (cached)
        assert ctx1 is ctx2


class TestBackwardCompatibility:
    """Test backward compatibility with old API."""
    
    def test_old_dataclass_aliases_work(self):
        """Old dataclass names should still work."""
        # Test that old names are available
        mock_context = MagicMock(spec=ssl.SSLContext)
        cached_ssl = CachedSSLContext(
            item=mock_context,
            created_at=time.time(),
            access_count=1,
            last_accessed=time.time()
        )
        assert cached_ssl.item is mock_context
        
        test_cert = b"test"
        cached_cert = CachedCertificate(
            item=test_cert,
            created_at=time.time(),
            access_count=1,
            last_accessed=time.time()
        )
        assert cached_cert.item == test_cert
    
    def test_ssl_cache_config_exists(self):
        """SSLCacheConfig should still exist for backward compatibility."""
        config = SSLCacheConfig()
        assert config.enabled is False
        assert config.max_size == 100


class TestCertificateCacheIntegration:
    """Test certificate cache integration with get_certificate()."""
    
    @patch('mapepire_python.ssl.socket.create_connection')
    @patch('mapepire_python.ssl_cache.should_use_cache')
    def test_get_certificate_uses_cache(self, mock_should_use_cache, mock_socket):
        """get_certificate() should use certificate cache when enabled."""
        from mapepire_python.ssl import get_certificate
        
        # Clear cache to start fresh
        clear_certificate_cache()
        
        # Mock server configuration
        server = DaemonServer(
            host="test.example.com",
            user="user1",
            password="pass1",
            port=8443,
            ssl_cache_enabled=True
        )
        
        # Mock the socket connection and SSL handshake
        mock_sock = MagicMock()
        mock_ssock = MagicMock()
        mock_socket.return_value.__enter__.return_value = mock_sock
        
        # Configure SSL socket mock
        mock_ssl_context = MagicMock()
        mock_ssl_context.wrap_socket.return_value.__enter__.return_value = mock_ssock
        mock_ssock.getpeercert.return_value = b"mock_cert_data"
        
        # Enable caching
        mock_should_use_cache.return_value = True
        
        # Mock ssl.DER_cert_to_PEM_cert
        with patch('mapepire_python.ssl.ssl.DER_cert_to_PEM_cert') as mock_convert:
            mock_convert.return_value = b"-----BEGIN CERTIFICATE-----\nMOCK_CERT\n-----END CERTIFICATE-----"
            
            # First call should miss cache and fetch certificate
            with patch('mapepire_python.ssl.ssl.create_default_context') as mock_create_context:
                mock_create_context.return_value = mock_ssl_context
                
                cert1 = get_certificate(server)
                stats_after_first = get_certificate_cache_stats()
                
                # Second call should hit cache
                cert2 = get_certificate(server)
                stats_after_second = get_certificate_cache_stats()
        
        # Verify caching behavior
        assert cert1 == cert2
        assert stats_after_first["hits"] == 0
        assert stats_after_first["misses"] == 1
        assert stats_after_second["hits"] == 1
        assert stats_after_second["misses"] == 1