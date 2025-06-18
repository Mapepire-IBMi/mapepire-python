"""
Tests for mapepire_python.client.sql_job module.
Simple tests using real IBM i server.
"""

from mapepire_python.client.sql_job import SQLJob


def test_sql_job_creation(ibmi_credentials):
    """Test basic SQLJob creation."""
    job = SQLJob(ibmi_credentials)
    assert job is not None
    assert job.creds == ibmi_credentials


def test_sql_job_context_manager(ibmi_credentials):
    """Test SQLJob as context manager."""
    with SQLJob(ibmi_credentials) as job:
        assert job is not None
        # Job should be connected


def test_sql_job_query_and_run(ibmi_credentials, simple_count_sql):
    """Test query_and_run method."""
    with SQLJob(ibmi_credentials) as job:
        result = job.query_and_run(simple_count_sql)

        assert result is not None
        assert isinstance(result, dict)
        assert "success" in result
        assert result["success"] is True
        assert "data" in result


def test_sql_job_query_context_manager(ibmi_credentials, simple_count_sql):
    """Test creating queries with context manager."""
    with SQLJob(ibmi_credentials) as job:
        with job.query(simple_count_sql) as query:
            result = query.run()

            assert result is not None
            assert isinstance(result, dict)
            assert result.get("success") is True


def test_sql_job_multiple_queries(ibmi_credentials):
    """Test multiple queries on same job."""
    with SQLJob(ibmi_credentials) as job:
        # First query
        result1 = job.query_and_run("SELECT COUNT(*) FROM sample.employee")

        # Second query
        result2 = job.query_and_run("SELECT COUNT(*) FROM sample.department")

        assert result1["success"] is True
        assert result2["success"] is True


def test_sql_job_with_parameters(ibmi_credentials):
    """Test SQL job with parameterized queries."""
    with SQLJob(ibmi_credentials) as job:
        result = job.query_and_run(
            "SELECT * FROM sample.employee WHERE empno = ?", {"parameters": ["000010"]}
        )

        assert result["success"] is True


def test_sql_job_error_handling(ibmi_credentials):
    """Test SQL job error handling with invalid query."""
    with SQLJob(ibmi_credentials) as job:
        result = job.query_and_run("SELECT * FROM nonexistent_table")

        # Should return error result, not raise exception
        assert isinstance(result, dict)
        assert result.get("success") is False
        assert "error" in result


def test_sql_job_large_result_set(ibmi_credentials):
    """Test handling larger result sets."""
    with SQLJob(ibmi_credentials) as job:
        result = job.query_and_run("SELECT * FROM sample.employee", rows_to_fetch=10)

        assert result["success"] is True
        if result.get("has_results"):
            assert "data" in result
            assert isinstance(result["data"], list)


def test_sql_job_cl_command(ibmi_credentials):
    """Test CL command execution if supported."""
    with SQLJob(ibmi_credentials) as job:
        # Simple CL command
        result = job.query_and_run("WRKACTJOB", opts={"isClCommand": True})

        # This might fail depending on server configuration
        # Just ensure it doesn't crash the connection
        assert isinstance(result, dict)
