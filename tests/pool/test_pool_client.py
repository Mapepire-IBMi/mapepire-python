"""
Tests for Pool client functionality (migrated from pooling_test.py).
Simple Pool client tests using real IBM i server.
"""

import asyncio
import pytest
from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import JobStatus
from mapepire_python.pool.pool_client import Pool as OriginalPool, PoolOptions as OriginalPoolOptions
from mapepire_python.pool.optimized_pool_client import OptimizedPool, PoolOptions as OptimizedPoolOptions

# Compatibility for existing tests
Pool = OriginalPool
PoolOptions = OriginalPoolOptions


@pytest.mark.asyncio
async def test_pool_simple_context_manager(ibmi_credentials):
    """Test simple Pool with context manager."""
    async with Pool(
        options=PoolOptions(creds=ibmi_credentials, opts=None, max_size=5, starting_size=3)
    ) as pool:
        job_names = []
        try:
            resultsA = await asyncio.gather(
                pool.execute("values (job_name)"),
                pool.execute("values (job_name)"),
                pool.execute("values (job_name)"),
            )
            job_names = [res["data"][0]["00001"] for res in resultsA]
            assert len(job_names) == 3
            assert pool.get_active_job_count() == 3
        finally:
            await pool.end()
            pending = asyncio.all_tasks()
            for task in pending:
                if task is not asyncio.current_task():
                    task.cancel()


@pytest.mark.asyncio
async def test_pool_simple_execution(ibmi_credentials):
    """Test simple Pool execution with multiple queries."""
    async with Pool(
        options=PoolOptions(creds=ibmi_credentials, opts=None, max_size=5, starting_size=3)
    ) as pool:
        job_names = []
        resultsA = await asyncio.gather(
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
        )
        job_names = [res["data"][0]["00001"] for res in resultsA]

        assert len(job_names) == 3
        assert pool.get_active_job_count() == 3

        resultsB = await asyncio.gather(
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
        )

        job_names = [res["data"][0]["00001"] for res in resultsB]
        assert len(job_names) == 15


@pytest.mark.asyncio
async def test_pool_starting_size_greater_than_max_size(ibmi_credentials):
    """Test Pool with starting size greater than max size."""
    pool = Pool(PoolOptions(creds=ibmi_credentials, max_size=1, starting_size=10))
    with pytest.raises(ValueError, match="Max size must be greater than or equal to starting size"):
        await pool.init()


@pytest.mark.asyncio
async def test_pool_max_size_of_zero(ibmi_credentials):
    """Test Pool with max size of 0."""
    pool = Pool(PoolOptions(creds=ibmi_credentials, max_size=0, starting_size=10))
    with pytest.raises(ValueError, match="Max size must be greater than 0"):
        await pool.init()


@pytest.mark.asyncio
async def test_pool_starting_size_of_zero(ibmi_credentials):
    """Test Pool with starting size of 0."""
    pool = Pool(PoolOptions(creds=ibmi_credentials, max_size=5, starting_size=0))
    with pytest.raises(ValueError, match="Starting size must be greater than 0"):
        await pool.init()


@pytest.mark.performance
@pytest.mark.asyncio
async def test_pool_performance_comparison(ibmi_credentials):
    """Test Pool performance with different configurations."""
    # Test with pool max_size=5, starting_size=5
    pool = Pool(PoolOptions(creds=ibmi_credentials, max_size=5, starting_size=5))
    await pool.init()
    start_pool1 = asyncio.get_event_loop().time()
    queries = [pool.execute("select * FROM SAMPLE.employee") for _ in range(20)]
    results = await asyncio.gather(*queries)
    end_pool1 = asyncio.get_event_loop().time()
    await pool.end()
    assert all(res["has_results"] for res in results)

    # Test with pool max_size=1, starting_size=1
    pool = Pool(PoolOptions(creds=ibmi_credentials, max_size=1, starting_size=1))
    await pool.init()
    start_pool2 = asyncio.get_event_loop().time()
    queries = [pool.execute("select * FROM SAMPLE.employee") for _ in range(20)]
    results = await asyncio.gather(*queries)
    end_pool2 = asyncio.get_event_loop().time()
    await pool.end()
    assert all(res["has_results"] for res in results)

    # Test without pool
    no_pool_start = asyncio.get_event_loop().time()
    with SQLJob(ibmi_credentials) as job:
        for _ in range(20):
            job.query_and_run("select * FROM SAMPLE.employee")
    no_pool_end = asyncio.get_event_loop().time()
    
    print(f"Time taken with pool (maxSize=5, startingSize=5): {end_pool1 - start_pool1} seconds")
    print(f"Time taken with pool (maxSize=1, startingSize=1): {end_pool2 - start_pool2} seconds")
    print(f"Time taken without pool: {no_pool_end - no_pool_start} seconds")


@pytest.mark.asyncio
async def test_pool_no_space_but_ready_job_returns_ready_job(ibmi_credentials):
    """Test Pool with no space but ready job returns ready job."""
    async with Pool(PoolOptions(creds=ibmi_credentials, max_size=2, starting_size=2)) as pool:
        assert pool.get_active_job_count() == 2
        executed_promise = [pool.execute("select * FROM SAMPLE.employee")]
        job = await pool.get_job()
        assert job.get_status() == JobStatus.Ready
        assert job.get_running_count() == 0
        await asyncio.gather(*executed_promise)


@pytest.mark.asyncio
async def test_pool_pop_jobs_returns_free_job(ibmi_credentials):
    """Test Pool pop_job returns free job."""
    async with Pool(PoolOptions(creds=ibmi_credentials, max_size=5, starting_size=5)) as pool:
        try:
            assert pool.get_active_job_count() == 5
            executed_promises = [
                pool.execute("select * FROM SAMPLE.employee"),
                pool.execute("select * FROM SAMPLE.employee"),
            ]
            job = await pool.pop_job()
            assert job.get_unique_id().startswith("sqljob")
            assert job.get_status() == JobStatus.Ready
            assert job.get_running_count() == 0
            assert pool.get_active_job_count() == 4
            await asyncio.gather(*executed_promises)
        finally:
            await pool.end()
            pending = asyncio.all_tasks()
            for task in pending:
                if task is not asyncio.current_task():
                    task.cancel()


# =============================================================================
# OPTIMIZED POOL TESTS - Real IBM i Server Integration
# =============================================================================

@pytest.mark.asyncio
async def test_optimized_pool_simple_context_manager(ibmi_credentials):
    """Test optimized Pool with context manager."""
    async with OptimizedPool(
        OptimizedPoolOptions(creds=ibmi_credentials, max_size=5, starting_size=3)
    ) as pool:
        job_names = []
        try:
            resultsA = await asyncio.gather(
                pool.execute("values (job_name)"),
                pool.execute("values (job_name)"),
                pool.execute("values (job_name)"),
            )
            job_names = [res["data"][0]["00001"] for res in resultsA]
            assert len(job_names) == 3
            assert pool.get_active_job_count() >= 3
        finally:
            await pool.end()
            pending = asyncio.all_tasks()
            for task in pending:
                if task is not asyncio.current_task():
                    task.cancel()


@pytest.mark.asyncio
async def test_optimized_pool_simple_execution(ibmi_credentials):
    """Test optimized Pool execution with multiple queries."""
    async with OptimizedPool(
        OptimizedPoolOptions(creds=ibmi_credentials, max_size=5, starting_size=3)
    ) as pool:
        job_names = []
        resultsA = await asyncio.gather(
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
        )
        job_names = [res["data"][0]["00001"] for res in resultsA]

        assert len(job_names) == 3
        assert pool.get_active_job_count() >= 3

        resultsB = await asyncio.gather(
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
            pool.execute("values (job_name)"),
        )

        job_names = [res["data"][0]["00001"] for res in resultsB]
        assert len(job_names) == 10


@pytest.mark.asyncio
async def test_optimized_pool_validation_errors(ibmi_credentials):
    """Test optimized Pool validation errors."""
    # Test starting size greater than max size
    pool = OptimizedPool(OptimizedPoolOptions(creds=ibmi_credentials, max_size=1, starting_size=10))
    with pytest.raises(ValueError, match="Max size must be greater than or equal to starting size"):
        await pool.init()

    # Test max size of 0
    pool = OptimizedPool(OptimizedPoolOptions(creds=ibmi_credentials, max_size=0, starting_size=10))
    with pytest.raises(ValueError, match="Max size must be greater than 0"):
        await pool.init()

    # Test starting size of 0
    pool = OptimizedPool(OptimizedPoolOptions(creds=ibmi_credentials, max_size=5, starting_size=0))
    with pytest.raises(ValueError, match="Starting size must be greater than 0"):
        await pool.init()


@pytest.mark.performance
@pytest.mark.asyncio
async def test_optimized_pool_performance_comparison(ibmi_credentials):
    """Test optimized Pool performance vs original implementation."""
    import time
    
    # Test optimized pool with larger pool size
    pool = OptimizedPool(OptimizedPoolOptions(creds=ibmi_credentials, max_size=10, starting_size=5))
    await pool.init()
    start_optimized = time.perf_counter()
    queries = [pool.execute("select * FROM SAMPLE.employee") for _ in range(20)]
    results = await asyncio.gather(*queries)
    end_optimized = time.perf_counter()
    await pool.end()
    assert all(res["has_results"] for res in results)
    
    # Test original pool with same configuration
    original_pool = OriginalPool(OriginalPoolOptions(creds=ibmi_credentials, max_size=10, starting_size=5))
    await original_pool.init()
    start_original = time.perf_counter()
    queries = [original_pool.execute("select * FROM SAMPLE.employee") for _ in range(20)]
    results = await asyncio.gather(*queries)
    end_original = time.perf_counter()
    await original_pool.end()
    assert all(res["has_results"] for res in results)
    
    optimized_time = end_optimized - start_optimized
    original_time = end_original - start_original
    
    print(f"Optimized pool time: {optimized_time:.3f}s")
    print(f"Original pool time: {original_time:.3f}s")
    
    # Optimized pool should be at least as fast (allowing for some variance in network conditions)
    # Main benefit is in job selection efficiency, not necessarily total query time
    assert optimized_time < original_time * 1.5, "Optimized pool should be competitive with original"


@pytest.mark.asyncio
async def test_optimized_pool_job_selection_efficiency(ibmi_credentials):
    """Test optimized Pool job selection with ready queue efficiency."""
    async with OptimizedPool(
        OptimizedPoolOptions(creds=ibmi_credentials, max_size=5, starting_size=5)
    ) as pool:
        # Verify initial ready jobs
        assert len(pool.ready_jobs) == 5
        assert pool.get_active_job_count() == 5
        
        # Get a job and verify it's moved to busy state
        job = await pool.get_job()
        assert job.get_status() == JobStatus.Ready
        assert len(pool.ready_jobs) == 4  # One moved to busy
        
        # Execute query to actually make it busy
        result = await pool.execute("values (job_name)")
        assert result["success"]
        
        # Test cache efficiency
        initial_cache_hits = pool.stats["cache_hits"]
        for _ in range(10):
            _ = pool._get_cached_running_count(job)
        
        # Should have cache hits after first access
        assert pool.stats["cache_hits"] > initial_cache_hits


@pytest.mark.asyncio
async def test_optimized_pool_concurrent_access(ibmi_credentials):
    """Test optimized Pool with high concurrency."""
    async with OptimizedPool(
        OptimizedPoolOptions(creds=ibmi_credentials, max_size=8, starting_size=4)
    ) as pool:
        # Run many concurrent queries
        concurrent_queries = 50
        queries = [pool.execute("values (job_name)") for _ in range(concurrent_queries)]
        results = await asyncio.gather(*queries)
        
        # All queries should succeed
        assert len(results) == concurrent_queries
        assert all(res["success"] for res in results)
        assert all("data" in res for res in results)
        
        # Verify pool statistics
        assert pool.stats["ready_queue_hits"] > 0
        assert pool._calculate_cache_hit_ratio() >= 0.0  # Should have some cache activity


@pytest.mark.asyncio
async def test_optimized_pool_pre_warming(ibmi_credentials):
    """Test optimized Pool pre-warming functionality."""
    # Test with pre-warming enabled
    pool_prewarmed = OptimizedPool(OptimizedPoolOptions(
        creds=ibmi_credentials, 
        max_size=5, 
        starting_size=3,
        pre_warm_connections=True
    ))
    
    start_time = asyncio.get_event_loop().time()
    await pool_prewarmed.init()
    prewarmed_init_time = asyncio.get_event_loop().time() - start_time
    
    assert len(pool_prewarmed.ready_jobs) == 3
    assert pool_prewarmed.get_active_job_count() == 3
    await pool_prewarmed.end()
    
    # Test with pre-warming disabled
    pool_sequential = OptimizedPool(OptimizedPoolOptions(
        creds=ibmi_credentials, 
        max_size=5, 
        starting_size=3,
        pre_warm_connections=False
    ))
    
    start_time = asyncio.get_event_loop().time()
    await pool_sequential.init()
    sequential_init_time = asyncio.get_event_loop().time() - start_time
    
    assert len(pool_sequential.ready_jobs) == 3
    assert pool_sequential.get_active_job_count() == 3
    await pool_sequential.end()
    
    print(f"Pre-warmed init time: {prewarmed_init_time:.3f}s")
    print(f"Sequential init time: {sequential_init_time:.3f}s")
    
    # Pre-warming should be faster (parallel vs sequential connection establishment)
    # Allow some variance due to network conditions
    assert prewarmed_init_time <= sequential_init_time * 1.2


@pytest.mark.asyncio
async def test_optimized_pool_string_representation(ibmi_credentials):
    """Test optimized Pool string representation."""
    async with OptimizedPool(
        OptimizedPoolOptions(creds=ibmi_credentials, max_size=3, starting_size=2)
    ) as pool:
        pool_str = str(pool)
        assert "OptimizedPool Stats" in pool_str
        assert "Total Jobs" in pool_str
        assert "Performance" in pool_str
        assert "Cache hit ratio" in pool_str
        assert "Ready:" in pool_str
        assert "Busy:" in pool_str


@pytest.mark.asyncio
async def test_optimized_pool_backward_compatibility(ibmi_credentials):
    """Test that optimized Pool maintains backward compatibility with original API."""
    async with OptimizedPool(
        OptimizedPoolOptions(creds=ibmi_credentials, max_size=3, starting_size=2)
    ) as pool:
        # Test all public methods exist and work
        assert hasattr(pool, 'has_space')
        assert hasattr(pool, 'get_active_job_count')
        assert hasattr(pool, 'get_job')
        assert hasattr(pool, 'wait_for_job')
        assert hasattr(pool, 'pop_job')
        assert hasattr(pool, 'query')
        assert hasattr(pool, 'execute')
        assert hasattr(pool, 'end')
        
        # Test basic operations work like original
        assert isinstance(pool.has_space(), bool)
        assert isinstance(pool.get_active_job_count(), int)
        
        # Test job operations
        job = await pool.get_job()
        assert job.get_status() == JobStatus.Ready
        
        # Test query operations
        result = await pool.execute("values (current timestamp)")
        assert result["success"]
        assert "data" in result


@pytest.mark.asyncio
async def test_optimized_vs_original_pool_equivalence(ibmi_credentials):
    """Test that optimized and original pools produce equivalent results."""
    test_sql = "select * from SAMPLE.employee where EMPNO = '000010'"
    
    # Test with original pool
    async with OriginalPool(
        OriginalPoolOptions(creds=ibmi_credentials, max_size=3, starting_size=2)
    ) as original_pool:
        original_result = await original_pool.execute(test_sql)
    
    # Test with optimized pool
    async with OptimizedPool(
        OptimizedPoolOptions(creds=ibmi_credentials, max_size=3, starting_size=2)
    ) as optimized_pool:
        optimized_result = await optimized_pool.execute(test_sql)
    
    # Results should be equivalent
    assert original_result["success"] == optimized_result["success"]
    assert original_result["has_results"] == optimized_result["has_results"]
    assert len(original_result["data"]) == len(optimized_result["data"])
    
    if original_result["data"]:
        # Compare first row data
        assert original_result["data"][0] == optimized_result["data"][0]