"""
Tests for QueryManager functionality (migrated from query_manager_test.py).
Simple QueryManager tests using real IBM i server.
"""

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.query_manager import QueryManager


def test_query_manager_basic(ibmi_credentials):
    """Test basic QueryManager functionality."""
    # connection logic
    job = SQLJob()
    job.connect(ibmi_credentials)

    # Query Manager
    query_manager = QueryManager(job)

    # create a unique query
    query = query_manager.create_query("select * from sample.employee")

    # run query
    result = query_manager.run_query(query)

    assert result["success"]
    job.close()


def test_query_manager_context_manager_with_query(ibmi_credentials):
    """Test QueryManager with context manager and query context manager."""
    with SQLJob(ibmi_credentials) as sql_job:
        query_manager = QueryManager(sql_job)
        with query_manager.create_query("select * from sample.employee") as query:
            result = query_manager.run_query(query, rows_to_fetch=1)
            assert result["success"]


def test_query_manager_with_query_and_run(ibmi_credentials):
    """Test QueryManager with query_and_run shortcut."""
    with SQLJob(creds=ibmi_credentials) as job:
        query_manager = QueryManager(job)
        res = query_manager.query_and_run("select * from sample.employee")
        assert res["success"] == True


def test_query_manager_with_job_context_manager(ibmi_credentials):
    """Test QueryManager with SQLJob context manager."""
    with SQLJob() as job:
        job.connect(ibmi_credentials)

        query_manager = QueryManager(job)
        query = query_manager.create_query("select * from sample.department")
        result = query_manager.run_query(query)
        assert result["success"]


def test_query_manager_simple_v2(ibmi_credentials):
    """Test QueryManager simple functionality with explicit close."""
    with SQLJob(ibmi_credentials) as job:
        query_manager = QueryManager(job)
        query = query_manager.create_query("select * from sample.employee")
        result = query_manager.run_query(query, rows_to_fetch=5)
        assert result["success"] == True
        assert result["is_done"] == False
        assert result["has_results"] == True
        query.close()


def test_query_manager_large_dataset(ibmi_credentials):
    """Test QueryManager with large dataset."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    query_manager = QueryManager(job)
    query = query_manager.create_query("select * from sample.employee")

    result = query_manager.run_query(query, rows_to_fetch=30)
    query.close()
    job.close()

    assert result["success"] == True
    assert result["is_done"] == False
    assert result["has_results"] == True
    assert len(result["data"]) == 30


def test_query_manager_query_and_run_shortcut(ibmi_credentials):
    """Test QueryManager query_and_run shortcut method."""
    with SQLJob(ibmi_credentials) as job:
        query_manager = QueryManager(job)
        result = query_manager.query_and_run("select * from sample.employee", rows_to_fetch=5)
        assert result["success"] == True
        assert result["is_done"] == False
        assert result["has_results"] == True