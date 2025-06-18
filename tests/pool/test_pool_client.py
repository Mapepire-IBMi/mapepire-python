"""
Tests for Pool client functionality (migrated from pooling_test.py).
Simple Pool client tests using real IBM i server.
"""

import asyncio
import pytest
from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import JobStatus
from mapepire_python.pool.pool_client import Pool, PoolOptions


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