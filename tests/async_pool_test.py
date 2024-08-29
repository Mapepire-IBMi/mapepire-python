# Fetch environment variables
import asyncio
import os

import pytest

from mapepire_python.client.query_manager import QueryManager
from mapepire_python.client.sql_job import SQLJob
from mapepire_python.pool.pool_job import PoolJob
from mapepire_python.types import DaemonServer, QueryOptions

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
async def test_pool():
    async with PoolJob(creds=creds) as job:
        query = job.query('select * from sample.employee')
        query2 = job.query('select * from sample.department')
        res = await query.run()
        res2 = await query2.run()
        assert res['success'] == True
        assert res2['success'] == True
        
    async with PoolJob(creds=creds) as pool_job:
        async with pool_job.query('select * from sample.employee') as query:
          res = await query.run(rows_to_fetch=1)
        
def test_pool2():
    with SQLJob(creds=creds) as job:
        query = job.query('select * from sample.employee')
        query2 = job.query('select * from sample.department')
        res = query.run()
        res2 = query2.run()
        assert res['success'] == True
        assert res2['success'] == True

@pytest.mark.asyncio
async def test_simple_dict():
    creds_dict = {
        'host': server,
        'user': user,
        'port': port,
        'password': password,
        'port': port,
        'ignoreUnauthorized': True
    }
    job = PoolJob()
    await job.connect(creds_dict)
    query = job.query("select * from sample.employee")
    result = await query.run(rows_to_fetch=5)
    await job.close()
    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True
        
@pytest.mark.asyncio
async def test_simple2():
    job = PoolJob()
    job.enable_local_trace_data()
    _ = await job.connect(creds)
    query = job.query('select * from sample.employee')
    result = await query.run(rows_to_fetch=5)
    await job.close()
    assert result['success'] == True
    assert result['is_done'] == False
    assert result['has_results'] == True
        
    
@pytest.mark.asyncio
async def test_simple():
    job = PoolJob()
    _ = await job.connect(creds)
    query = job.query('select * from sample.employee')
    result = await query.run(rows_to_fetch=5)
    await job.close()
    assert result['success'] == True
    assert result['is_done'] == False
    assert result['has_results'] == True
    
@pytest.mark.asyncio
async def test_query_large_dataset():
    job = PoolJob()
    _ = await job.connect(creds)
    query = job.query('select * from sample.employee')
    result = await query.run(rows_to_fetch=30)
    await job.close()
    
    assert result['success'] == True
    assert result['is_done'] == False
    assert result['has_results'] == True
    assert len(result['data']) == 30
    
@pytest.mark.asyncio
async def test_run_query_terse_format():
    job = PoolJob()
    _ = await job.connect(creds)
    opts = QueryOptions(
        isTerseResults=True
    )
    query = job.query('select * from sample.employee', opts=opts)
    result = await query.run(rows_to_fetch=5)
    await job.close()
    
    assert result['success'] == True
    assert result['is_done'] == False
    assert result['has_results'] == True
    assert 'metadata' in result and result['metadata'], "The 'metadata' key is missing or has no data"
    
@pytest.mark.asyncio
async def test_invalid_query():
    job = PoolJob()
    _ = await job.connect(creds)
    query = job.query('select * from sample.notreal')
    try:
        result = await query.run(rows_to_fetch=5)
        raise Exception("error not raised")
    except Exception as e:
        assert e.args[0]
        assert 'error' in e.args[0]
        assert '*FILE not found.'  in e.args[0]['error']
    await job.close()
    
@pytest.mark.asyncio
async def test_query_edge_cases():
    job = PoolJob()
    _ = await job.connect(creds)
    query = job.query('')  # empty string
    try:
        _ = await query.run(rows_to_fetch=1)
        raise Exception('no error raised')
    except Exception as e:
        assert e.args[0]
        assert 'error' in e.args[0]
        assert 'A string parameter value with zero length was detected.' in e.args[0]['error']
    
@pytest.mark.asyncio
async def test_run_sql_query_with_edge_case_inputs():
    job = PoolJob()
    await job.connect(creds)
    
    # Test empty string query
    query = job.query('')
    with pytest.raises(Exception) as excinfo:
        await query.run(rows_to_fetch=1)
    assert 'A string parameter value with zero length was detected.' in str(excinfo.value)
    
    # Test non-string query
    with pytest.raises(Exception) as excinfo:
        query = job.query(666)
        await query.run(rows_to_fetch=1)
    assert 'Token 666 was not valid' in str(excinfo.value)
    
    # Test invalid token query
    query = job.query('a')
    with pytest.raises(Exception) as excinfo:
        await query.run(rows_to_fetch=1)
    assert 'Token A was not valid.' in str(excinfo.value)
    
    # Test long invalid token query
    long_invalid_query = "aeriogfj304tq34projqwe'fa;sdfaSER90Q243RSDASDAFQ#4dsa12$$$YS" * 10
    query = job.query(long_invalid_query)
    with pytest.raises(Exception) as excinfo:
        await query.run(rows_to_fetch=1)
    assert 'Token AERIOGFJ304TQ34PROJQWE was not valid.' in str(excinfo.value)
    
    # Test valid query with zero rows to fetch
    query = job.query("SELECT * FROM SAMPLE.employee")
    res = await query.run(rows_to_fetch=0)
    assert res['data'] == [], "Expected empty result set when rows_to_fetch is 0"
    
    # Test valid query with non-numeric rows to fetch
    # use async default rows_to_fetch == 100
    query = job.query("select * from sample.department")
    res = await query.run(rows_to_fetch='s')
    assert res['success']
    
    # Test valid query with negative rows to fetch
    query = job.query("select * from sample.department")
    res = await query.run(rows_to_fetch=-1)
    assert res['data'] == [], "Expected empty result set when rows_to_fetch < 0"
    
    # query.close()
    await job.close()
    
@pytest.mark.asyncio
async def test_drop_table():
    job = PoolJob()
    _ = await job.connect(creds)
    query = job.query('drop table sample.delete if exists')
    res = await query.run()
    assert res['has_results'] == False
    await job.close()
    
@pytest.mark.asyncio
async def test_fetch_more():
    job = PoolJob()
    _ = await job.connect(creds)
    query = job.query('select * from sample.employee')
    res = await query.run(rows_to_fetch=5)
    while not res['is_done']:
        res = await query.fetch_more(10)
        assert len(res['data']) > 0
    
    await job.close()
    assert res['is_done']
    
@pytest.mark.asyncio
async def test_prepare_statement():
    job = PoolJob()
    _ = await job.connect(creds)
    opts = QueryOptions(
        parameters=[500]
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    res = await query.run()
    assert res['success']
    assert len(res['data']) >= 17
    
@pytest.mark.asyncio
async def test_prepare_statement_terse():
    job = PoolJob()
    _ = await job.connect(creds)
    opts = QueryOptions(
        parameters=[500],
        isTerseResults=True
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    res = await query.run()
    assert res['success']
    assert len(res['data']) >= 17
    assert 'metadata' in res
    
@pytest.mark.asyncio
async def test_prepare_statement_mult_params():
    job = PoolJob()
    _ = await job.connect(creds)
    opts = QueryOptions(
        parameters=[500, 'PRES']
    )
    query = job.query('select * from sample.employee where bonus > ? and job = ?', opts=opts)
    res = await query.run()
    assert res['success']
    await job.close()
    
@pytest.mark.asyncio
async def test_prepare_statement_invalid_params():
    job = PoolJob()
    _ = await job.connect(creds)
    opts = QueryOptions(
        parameters=['jjfkdsajf']
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    with pytest.raises(Exception) as execinfo:
        res = await query.run()
    assert 'Data type mismatch. (Infinite or NaN)' in str(execinfo.value)
    
@pytest.mark.asyncio
async def test_prepare_statement_no_param():
    job = PoolJob()
    _ = await job.connect(creds)
    opts = QueryOptions(
        parameters=[],
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    with pytest.raises(Exception) as execinfo:
        res = await query.run()
    assert 'The number of parameter values set or registered does not match the number of parameters.' in str(execinfo.value)
    await job.close()
    
@pytest.mark.asyncio
async def test_prepare_statement_too_many():
    job = PoolJob()
    _ = await job.connect(creds)
    opts = QueryOptions(
        parameters=[500, 'hello']
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    with pytest.raises(Exception) as execinfo:
        res = await query.run()
    assert 'Descriptor index not valid. (2>1)' in str(execinfo.value)
    
@pytest.mark.asyncio
async def test_prepare_statement_invalid_data():
    job = PoolJob()
    _ = await job.connect(creds)
    opts = QueryOptions(
        parameters=[{'bonus': 500}]
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    with pytest.raises(Exception) as execinfo:
        res = await query.run()
    assert 'JsonObject' in str(execinfo.value)
    
@pytest.mark.asyncio
async def test_run_from_job():
    job = PoolJob()
    _ = await job.connect(creds)
    res = await job.query_and_run('select * from sample.employee')
    assert res['success'] 
    
@pytest.mark.asyncio
async def test_multiple_statements():
    job = PoolJob()
    _ = await job.connect(creds)

    resA = await job.query("select * from sample.department").run()
    assert resA["success"] is True

    resB = await job.query("select * from sample.employee").run()
    assert resB["success"] is True

    await job.close()
    
    
    

