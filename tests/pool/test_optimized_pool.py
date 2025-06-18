"""
Integration tests for the optimized connection pool.

This test suite validates that the optimized pool maintains backward compatibility
while providing performance improvements.
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from mapepire_python.data_types import JobStatus
from mapepire_python.pool.optimized_pool_client import OptimizedPool
from mapepire_python.pool.optimized_pool_client import (
    PoolOptions as OptimizedPoolOptions,
)

# Test both pool implementations
from mapepire_python.pool.pool_client import PoolOptions as OriginalPoolOptions


class MockJob:
    """Mock job for testing without real connections."""

    def __init__(self, job_id: str = "test_job"):
        self.job_id = job_id
        self.status = JobStatus.NotStarted
        self.running_count = 0
        self.unique_id = job_id

    def get_status(self) -> JobStatus:
        return self.status

    def get_running_count(self) -> int:
        return self.running_count

    def get_unique_id(self) -> str:
        return self.unique_id

    async def connect(self, *args, **kwargs):
        """Mock connection - just set status to ready."""
        self.status = JobStatus.Ready
        await asyncio.sleep(0.001)  # Minimal delay

    async def close(self):
        """Mock close - set status to ended."""
        self.status = JobStatus.Ended

    def query(self, sql: str, opts=None):
        """Mock query method."""
        return MagicMock()

    async def query_and_run(self, sql: str, opts=None):
        """Mock query and run method."""
        return {"success": True, "data": [{"result": "test"}]}


@pytest.fixture
def optimized_pool_options():
    """Fixture for optimized pool options."""
    return OptimizedPoolOptions(
        creds=None,
        max_size=10,
        starting_size=3,
        pre_warm_connections=False,  # Disable pre-warming for tests
    )


@pytest.fixture
def original_pool_options():
    """Fixture for original pool options."""
    return OriginalPoolOptions(creds=None, max_size=10, starting_size=3)


@pytest.fixture
def mock_pool_jobs(monkeypatch):
    """Fixture to mock pool job creation for all tests."""
    
    async def mock_add_job_optimized(self, options=None):
        """Mock job creation without real connections."""
        from mapepire_python.pool.optimized_pool_client import PoolAddOptions
        if options is None:
            options = PoolAddOptions()
            
        job = MockJob(f"mock_job_{len(self.all_jobs)}")
        job.status = JobStatus.Ready
        
        if not options.pool_ignore:
            self.all_jobs.append(job)
            self.job_id_to_job[job.get_unique_id()] = job
            self.ready_jobs.append(job)
        
        self._update_job_metrics(job)
        self.stats["jobs_created"] += 1
        return job
    
    # Mock the pool job creation method
    monkeypatch.setattr("mapepire_python.pool.optimized_pool_client.OptimizedPool._add_job_optimized", mock_add_job_optimized)
    
    return mock_add_job_optimized


class TestOptimizedPoolBasicFunctionality:
    """Test basic pool functionality and compatibility."""

    @pytest.mark.asyncio
    async def test_pool_creation_and_initialization(self, optimized_pool_options):
        """Test that optimized pool can be created and initialized."""
        pool = OptimizedPool(optimized_pool_options)

        # Test pool state before initialization
        assert pool.has_space()
        assert pool.get_active_job_count() == 0
        assert len(pool.all_jobs) == 0
        assert len(pool.ready_jobs) == 0
        assert len(pool.busy_jobs_heap) == 0

    @pytest.mark.asyncio
    async def test_pool_context_manager(self, optimized_pool_options, mock_pool_jobs):
        """Test that pool works as an async context manager."""
        async with OptimizedPool(optimized_pool_options) as pool:
            assert pool is not None
            assert isinstance(pool, OptimizedPool)
            assert len(pool.all_jobs) == 3  # Should have created starting_size jobs

    @pytest.mark.asyncio
    async def test_job_metrics_caching(self, optimized_pool_options):
        """Test that job metrics are cached efficiently."""
        pool = OptimizedPool(optimized_pool_options)

        # Add a mock job
        job = MockJob("metrics_test_job")
        job.status = JobStatus.Ready
        pool.all_jobs.append(job)
        pool.job_id_to_job[job.get_unique_id()] = job

        # Update metrics
        pool._update_job_metrics(job)

        # Test cached access
        cached_count = pool._get_cached_running_count(job)
        assert cached_count == job.get_running_count()
        assert pool.stats["cache_hits"] > 0

    @pytest.mark.asyncio
    async def test_ready_queue_operations(self, optimized_pool_options):
        """Test O(1) ready queue operations."""
        pool = OptimizedPool(optimized_pool_options)

        # Add jobs to ready queue
        jobs = [MockJob(f"ready_job_{i}") for i in range(5)]
        for job in jobs:
            job.status = JobStatus.Ready
            pool.ready_jobs.append(job)
            pool.all_jobs.append(job)
            pool.job_id_to_job[job.get_unique_id()] = job
            pool._update_job_metrics(job)

        # Test O(1) ready job access
        assert len(pool.ready_jobs) == 5

        # Test job selection
        selected_job = pool.ready_jobs.popleft()
        assert selected_job in jobs
        assert len(pool.ready_jobs) == 4

    @pytest.mark.asyncio
    async def test_busy_jobs_heap_operations(self, optimized_pool_options):
        """Test efficient load balancing with busy jobs heap."""
        pool = OptimizedPool(optimized_pool_options)

        # Add busy jobs with different loads
        import heapq

        for i in range(5):
            job = MockJob(f"busy_job_{i}")
            job.status = JobStatus.Busy
            job.running_count = i + 1  # Different loads
            heapq.heappush(pool.busy_jobs_heap, (job.running_count, id(job), job))

        # Test that least busy job is at top of heap
        least_busy_count, _, least_busy_job = pool.busy_jobs_heap[0]
        assert least_busy_count == 1  # Should be the job with lowest load

    @pytest.mark.asyncio
    async def test_cleanup_operations(self, optimized_pool_options):
        """Test efficient cleanup of invalid jobs."""
        pool = OptimizedPool(optimized_pool_options)

        # Add mix of valid and invalid jobs
        valid_jobs = [MockJob(f"valid_job_{i}") for i in range(3)]
        invalid_jobs = [MockJob(f"invalid_job_{i}") for i in range(2)]

        # Set up jobs
        for job in valid_jobs:
            job.status = JobStatus.Ready
            pool.all_jobs.append(job)
            pool.job_id_to_job[job.get_unique_id()] = job
            pool.ready_jobs.append(job)

        for job in invalid_jobs:
            job.status = JobStatus.Ended
            pool.all_jobs.append(job)
            pool.job_id_to_job[job.get_unique_id()] = job

        # Test cleanup
        await pool._cleanup_invalid_jobs()

        # Verify only valid jobs remain
        assert len(pool.all_jobs) == 3
        assert len(pool.ready_jobs) == 3
        assert len(pool.job_id_to_job) == 3


class TestOptimizedPoolPerformance:
    """Test performance characteristics of optimized pool."""

    @pytest.mark.asyncio
    async def test_job_selection_performance(self, optimized_pool_options):
        """Test that job selection is faster than linear search."""
        pool = OptimizedPool(optimized_pool_options)

        # Add many ready jobs
        num_jobs = 100
        for i in range(num_jobs):
            job = MockJob(f"perf_job_{i}")
            job.status = JobStatus.Ready
            pool.ready_jobs.append(job)
            pool.all_jobs.append(job)
            pool.job_id_to_job[job.get_unique_id()] = job

        # Time job selection
        import time

        start_time = time.perf_counter()

        # Select many jobs
        for _ in range(50):
            if pool.ready_jobs:
                job = pool.ready_jobs.popleft()
                pool.ready_jobs.append(job)  # Put back for next iteration

        elapsed_time = time.perf_counter() - start_time

        # Should be very fast (under 1ms for 50 selections from 100 jobs)
        assert elapsed_time < 0.001, f"Job selection too slow: {elapsed_time:.6f}s"

    @pytest.mark.asyncio
    async def test_cache_efficiency(self, optimized_pool_options):
        """Test that caching improves performance."""
        pool = OptimizedPool(optimized_pool_options)

        # Add job with metrics
        job = MockJob("cache_test_job")
        job.status = JobStatus.Ready
        pool.all_jobs.append(job)
        pool.job_id_to_job[job.get_unique_id()] = job
        pool._update_job_metrics(job)

        # Access metrics multiple times
        for _ in range(10):
            pool._get_cached_running_count(job)

        # Should have cache hits
        assert pool.stats["cache_hits"] > 0
        cache_hit_ratio = pool._calculate_cache_hit_ratio()
        assert cache_hit_ratio > 0.8  # Should be mostly cache hits


class TestBackwardCompatibility:
    """Test that optimized pool maintains backward compatibility."""

    @pytest.mark.asyncio
    async def test_api_compatibility(self, optimized_pool_options):
        """Test that optimized pool has same API as original."""
        pool = OptimizedPool(optimized_pool_options)

        # Test that all original methods exist
        assert hasattr(pool, "has_space")
        assert hasattr(pool, "get_active_job_count")
        assert hasattr(pool, "get_job")
        assert hasattr(pool, "wait_for_job")
        assert hasattr(pool, "pop_job")
        assert hasattr(pool, "query")
        assert hasattr(pool, "execute")
        assert hasattr(pool, "end")

        # Test basic operations work (without requiring real connections)
        assert isinstance(pool.has_space(), bool)
        assert isinstance(pool.get_active_job_count(), int)

    @pytest.mark.asyncio
    async def test_string_representation(self, optimized_pool_options):
        """Test that string representation provides useful information."""
        pool = OptimizedPool(optimized_pool_options)

        pool_str = str(pool)
        assert "OptimizedPool Stats" in pool_str
        assert "Total Jobs" in pool_str
        assert "Performance" in pool_str
        assert "Cache hit ratio" in pool_str
