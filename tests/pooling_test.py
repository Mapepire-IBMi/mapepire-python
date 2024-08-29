import asyncio
import os

import pytest

from mapepire_python.client.query_manager import QueryManager
from mapepire_python.client.sql_job import SQLJob
from mapepire_python.pool.pool_client import Pool, PoolOptions
from mapepire_python.pool.pool_job import PoolJob
from mapepire_python.types import DaemonServer, JobStatus, QueryOptions

server = os.getenv('VITE_SERVER')
user = os.getenv('VITE_DB_USER')
password = os.getenv('VITE_DB_PASS')
port = os.getenv('VITE_DB_PORT')

# Check if environment variables are set
if not server or not user or not password:
    raise ValueError('One or more environment variables are missing.')


creds = DaemonServer(
    host=server,
    port=port,
    user=user,
    password=password,
    ignoreUnauthorized=True,
)


@pytest.mark.asyncio
async def test_simple_pool_cm():
    async with Pool(
        options=PoolOptions(
            creds=creds,
            opts=None,
            max_size=5,
            starting_size=3
        )
    ) as pool:
        job_names = []
        try:
            resultsA = await asyncio.gather(
                pool.execute('values (job_name)'),
                pool.execute('values (job_name)'),
                pool.execute('values (job_name)')
            )
            job_names = [res['data'][0]['00001'] for res in resultsA]
            
            assert len(job_names) == 3
            assert pool.get_active_job_count() == 3
        finally:
            # Ensure all tasks are completed before exiting
            pending = asyncio.all_tasks()
            if pending:
                for task in pending:
                    task.cancel()

@pytest.mark.asyncio
async def test_simple_pool():
    async with Pool(
        options=PoolOptions(
            creds=creds,
            opts=None,
            max_size=5,
            starting_size=3
        )
    ) as pool:
        
        job_names = []
        resultsA = await asyncio.gather(
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)')
        )
        job_names = [res['data'][0]['00001'] for res in resultsA]

        assert len(job_names) == 3
        
        assert pool.get_active_job_count() == 3
        
        resultsB = await asyncio.gather(
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
            pool.execute('values (job_name)'),
        )
        
        job_names = [res['data'][0]["00001"] for res in resultsB]
        assert len(job_names) == 15
    
    
@pytest.mark.asyncio
async def test_starting_size_greater_than_max_size():
    pool = Pool(PoolOptions(creds=creds, max_size=1, starting_size=10))
    with pytest.raises(ValueError, match="Max size must be greater than or equal to starting size"):
        await pool.init()
        
        
@pytest.mark.asyncio
async def test_max_size_of_0():
    pool = Pool(PoolOptions(creds=creds, max_size=0, starting_size=10))
    with pytest.raises(ValueError, match="Max size must be greater than 0"):
        await pool.init()

@pytest.mark.asyncio
async def test_starting_size_of_0():
    pool = Pool(PoolOptions(creds=creds, max_size=5, starting_size=0))
    with pytest.raises(ValueError, match="Starting size must be greater than 0"):
        await pool.init()

@pytest.mark.asyncio
async def test_performance_test():
    pool = Pool(PoolOptions(creds=creds, max_size=5, starting_size=5))
    await pool.init()
    start_pool1 = asyncio.get_event_loop().time()
    queries = [pool.execute("select * FROM SAMPLE.employee") for _ in range(20)]
    results = await asyncio.gather(*queries)
    end_pool1 = asyncio.get_event_loop().time()
    await pool.end()
    assert all(res['has_results'] for res in results)

    pool = Pool(PoolOptions(creds=creds, max_size=1, starting_size=1))
    await pool.init()
    start_pool2 = asyncio.get_event_loop().time()
    queries = [pool.execute("select * FROM SAMPLE.employee") for _ in range(20)]
    results = await asyncio.gather(*queries)
    end_pool2 = asyncio.get_event_loop().time()
    await pool.end()
    assert all(res['has_results'] for res in results)

    no_pool_start = asyncio.get_event_loop().time()
    # for _ in range(20):
    #     job = SQLJob()
    #     job.connect(creds)
    #     job.query_and_run("select * FROM SAMPLE.employee")
    #     job.close()
    with SQLJob(creds) as job:
        for _ in range(20):
            job.query_and_run("select * FROM SAMPLE.employee")
    no_pool_end = asyncio.get_event_loop().time()
    print(f"Time taken with pool (maxSize=5, startingSize=5): {end_pool1 - start_pool1} seconds")
    print(f"Time taken with pool (maxSize=1, startingSize=1): {end_pool2 - start_pool2} seconds")
    print(f"Time taken without pool: {no_pool_end - no_pool_start} seconds")
    # assert (end_pool2 - start_pool2) > (end_pool1 - start_pool1)
    # assert (no_pool_end - no_pool_start) > (end_pool2 - start_pool2)
    

@pytest.mark.asyncio
async def test_pool_with_no_space_but_ready_job_returns_ready_job():
    async with Pool(PoolOptions(creds=creds, max_size=2, starting_size=2)) as pool:
        assert pool.get_active_job_count() == 2
        executed_promise = [pool.execute("select * FROM SAMPLE.employee")]
        job = await pool.get_job()
        # job.enable_local_trace_data()
        assert job.get_status() == JobStatus.Ready
        assert job.get_running_count() == 0
        await asyncio.gather(*executed_promise)
        
        
        
# Functionality of pop_job() needs review 
@pytest.mark.asyncio
async def test_pop_jobs_returns_free_job():
    async with Pool(PoolOptions(creds=creds, max_size=5, starting_size=5)) as pool:
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
            # Ensure all tasks are completed before exiting
            pending = asyncio.all_tasks()
            if pending:
                for task in pending:
                    task.cancel()
            
        
# @pytest.mark.asyncio
# async def test_pop_job_with_pool_ignore():
#     async with Pool(PoolOptions(creds=creds, max_size=1, starting_size=1)) as pool:
#         try:
            
#             assert pool.get_active_job_count() == 1
            
#             executed_promises = [pool.execute("select * FROM SAMPLE.employee")]
            
#             # there is 1 job in pool, return that job
#             job = await pool.pop_job()
            
#             # the pool is empty, this will create a new job and add it to the pool
#             job2 = await pool.pop_job()
#             assert len(pool.jobs) == 1
#             assert job.get_status() == JobStatus.Ready
#             assert pool.get_active_job_count() == 1
#             await asyncio.gather(*executed_promises)
#         finally:
#             # Ensure all tasks are completed before exiting
#             pending = asyncio.all_tasks()
#             if pending:
#                 for task in pending:
#                     task.cancel()

    



# The following tests need further invesigation for tracking JobStatus and running tasks

# @pytest.mark.asyncio
# async def test_pool_with_no_space_no_ready_job_doesnt_increase_pool_size():
#     pool = Pool(PoolOptions(creds=creds, max_size=1, starting_size=1))
#     await pool.init()
#     add_job_spy = pool._add_job
#     assert pool.get_active_job_count() == 1
#     executed_promises = [
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#     ]
#     job = await pool.get_job()
#     # assert job.get_status() == JobStatus.Busy
#     print(job.get_running_count())
#     await asyncio.gather(*executed_promises)
#     print(job.get_running_count())
#     # assert not add_job_spy.called
#     # assert pool.get_active_job_count() == 1
#     await pool.end()



# @pytest.mark.asyncio
# async def test_pool_with_space_but_no_ready_job_adds_job_to_pool():
#     pool = Pool(PoolOptions(creds=creds, max_size=2, starting_size=1))
#     await pool.init()
#     add_job_spy = pool._add_job
#     assert pool.get_active_job_count() == 1
#     executed_promises = [
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#     ]
#     job = await pool.get_job()
#     assert job.get_status() == JobStatus.Busy
#     assert job.get_running_count() == 5
#     await asyncio.gather(*executed_promises)
#     assert add_job_spy.called
#     await pool.end()

# @pytest.mark.asyncio
# async def test_freeist_job_is_returned():
#     pool = Pool(PoolOptions(creds=creds, max_size=3, starting_size=3))
#     await pool.init()
#     executed_promises = [
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#         pool.execute("select * FROM SAMPLE.employee"),
#     ]
#     job = await pool.get_job()
#     job.enable_local_trace_data()
#     assert job.get_running_count() == 2
#     await asyncio.gather(*executed_promises)
#     await pool.end()
    
    
    