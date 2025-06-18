"""
Tests for mapepire_python.pool.pool_job module (migrated from async_pool_test.py).
Simple async tests using real IBM i server.
"""

import pytest
from mapepire_python.pool.pool_job import PoolJob
from mapepire_python.data_types import DaemonServer, QueryOptions
from mapepire_python.query_manager import QueryManager


@pytest.mark.asyncio
async def test_pool_job_basic(ibmi_credentials):
    """Test basic PoolJob functionality."""
    async with PoolJob(creds=ibmi_credentials) as job:
        query = job.query("select * from sample.employee")
        query2 = job.query("select * from sample.department") 
        res = await query.run()
        res2 = await query2.run()
        assert res["success"] is True
        assert res2["success"] is True


@pytest.mark.asyncio
async def test_pool_job_with_query_manager(ibmi_credentials):
    """Test PoolJob with QueryManager."""
    async with PoolJob(creds=ibmi_credentials) as pool_job:
        query_manager = QueryManager(pool_job)
        async with query_manager.create_query("select * from sample.employee") as query:
            res = await query.run(rows_to_fetch=1)
            assert res["success"] is True


@pytest.mark.asyncio 
async def test_pool_job_query_and_run(ibmi_credentials):
    """Test PoolJob query_and_run shortcut."""
    async with PoolJob(creds=ibmi_credentials) as pool_job:
        res = await pool_job.query_and_run("select * from sample.employee")
        assert res["success"] is True


@pytest.mark.asyncio
async def test_pool_job_query_manager_async(ibmi_credentials):
    """Test QueryManager async methods."""
    async with PoolJob(creds=ibmi_credentials) as pool_job:
        query_manager = QueryManager(pool_job)
        res = await query_manager.query_and_run_async("select * from sample.employee")
        assert res["success"] is True


@pytest.mark.asyncio
async def test_pool_job_context_manager_query(ibmi_credentials):
    """Test PoolJob with query context manager."""
    async with PoolJob(creds=ibmi_credentials) as pool_job:
        async with pool_job.query("select * from sample.employee") as query:
            res = await query.run(rows_to_fetch=1)
            assert res["success"] is True


@pytest.mark.asyncio
async def test_pool_job_dict_credentials(ibmi_credentials):
    """Test PoolJob with dictionary credentials."""
    creds_dict = {
        "host": ibmi_credentials["host"],
        "user": ibmi_credentials["user"],
        "port": ibmi_credentials["port"],
        "password": ibmi_credentials["password"],
        "ignoreUnauthorized": True,
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
async def test_pool_job_with_tracing(ibmi_credentials):
    """Test PoolJob with local tracing enabled."""
    job = PoolJob()
    job.enable_local_trace_data()
    _ = await job.connect(ibmi_credentials)
    query = job.query("select * from sample.employee")
    result = await query.run(rows_to_fetch=5)
    await job.close()
    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True


@pytest.mark.asyncio
async def test_pool_job_large_dataset(ibmi_credentials):
    """Test PoolJob with larger dataset."""
    job = PoolJob()
    _ = await job.connect(ibmi_credentials)
    query = job.query("select * from sample.employee")
    result = await query.run(rows_to_fetch=30)
    await job.close()
    
    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True
    assert len(result["data"]) <= 30  # Might be less if table has fewer rows


@pytest.mark.asyncio
async def test_pool_job_terse_format(ibmi_credentials):
    """Test PoolJob with terse results format."""
    job = PoolJob()
    _ = await job.connect(ibmi_credentials)
    opts = QueryOptions(isTerseResults=True)
    query = job.query("select * from sample.employee", opts=opts)
    result = await query.run(rows_to_fetch=5)
    await job.close()
    
    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True
    assert "metadata" in result and result["metadata"]


@pytest.mark.asyncio
async def test_pool_job_invalid_query(ibmi_credentials):
    """Test PoolJob error handling with invalid query."""
    job = PoolJob()
    _ = await job.connect(ibmi_credentials)
    query = job.query("select * from sample.notreal")
    try:
        result = await query.run(rows_to_fetch=5)
        raise Exception("error not raised")
    except Exception as e:
        assert e.args[0]
        assert "error" in e.args[0]
        assert "*FILE not found." in e.args[0]["error"]
    await job.close()


@pytest.mark.asyncio
async def test_pool_job_edge_cases(ibmi_credentials):
    """Test PoolJob with edge case inputs."""
    job = PoolJob()
    _ = await job.connect(ibmi_credentials)
    
    # Test empty query
    query = job.query("")
    try:
        _ = await query.run(rows_to_fetch=1)
        raise Exception("no error raised")
    except Exception as e:
        assert e.args[0]
        assert "error" in e.args[0]
        assert "A string parameter value with zero length was detected." in e.args[0]["error"]
    
    await job.close()


@pytest.mark.asyncio
async def test_pool_job_drop_table(ibmi_credentials):
    """Test PoolJob with DDL statement."""
    job = PoolJob()
    _ = await job.connect(ibmi_credentials)
    query = job.query("drop table sample.delete if exists")
    res = await query.run()
    assert res["has_results"] is False
    await job.close()


@pytest.mark.asyncio
async def test_pool_job_fetch_more(ibmi_credentials):
    """Test PoolJob fetch_more functionality."""
    job = PoolJob()
    _ = await job.connect(ibmi_credentials)
    query = job.query("select * from sample.employee")
    res = await query.run(rows_to_fetch=5)
    while not res["is_done"]:
        res = await query.fetch_more(10)
        assert len(res["data"]) >= 0
    
    await job.close()
    assert res["is_done"]


@pytest.mark.asyncio
async def test_pool_job_prepared_statements(ibmi_credentials):
    """Test PoolJob with prepared statements."""
    job = PoolJob()
    _ = await job.connect(ibmi_credentials)
    opts = QueryOptions(parameters=[500])
    query = job.query("select * from sample.employee where bonus > ?", opts=opts)
    res = await query.run()
    assert res["success"]
    await job.close()


@pytest.mark.asyncio
async def test_pool_job_prepared_statements_terse(ibmi_credentials):
    """Test PoolJob with prepared statements in terse format."""
    job = PoolJob()
    _ = await job.connect(ibmi_credentials)
    opts = QueryOptions(parameters=[500], isTerseResults=True)
    query = job.query("select * from sample.employee where bonus > ?", opts=opts)
    res = await query.run()
    assert res["success"]
    assert "metadata" in res
    await job.close()


@pytest.mark.asyncio
async def test_pool_job_multiple_parameters(ibmi_credentials):
    """Test PoolJob with multiple parameters."""
    job = PoolJob()
    _ = await job.connect(ibmi_credentials)
    opts = QueryOptions(parameters=[500, "PRES"])
    query = job.query("select * from sample.employee where bonus > ? and job = ?", opts=opts)
    res = await query.run()
    assert res["success"]
    await job.close()


@pytest.mark.asyncio
async def test_pool_job_parameter_errors(ibmi_credentials):
    """Test PoolJob parameter error handling."""
    job = PoolJob()
    _ = await job.connect(ibmi_credentials)
    
    # Invalid parameter type
    opts = QueryOptions(parameters=["jjfkdsajf"])
    query = job.query("select * from sample.employee where bonus > ?", opts=opts)
    with pytest.raises(Exception) as excinfo:
        res = await query.run()
    assert "Data type mismatch." in str(excinfo.value)
    
    await job.close()


@pytest.mark.asyncio
async def test_pool_job_multiple_statements(ibmi_credentials):
    """Test PoolJob with multiple sequential statements."""
    job = PoolJob()
    _ = await job.connect(ibmi_credentials)
    
    resA = await job.query("select * from sample.department").run()
    assert resA["success"] is True
    
    resB = await job.query("select * from sample.employee").run()
    assert resB["success"] is True
    
    await job.close()