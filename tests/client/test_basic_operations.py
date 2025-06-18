"""
Basic SQLJob operations tests (migrated from simple_test.py).
Simple tests using real IBM i server.
"""

import pytest
import re
from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import DaemonServer


def parse_sql_rc(message):
    """Helper to extract SQL return code from error message."""
    match = re.search(r"'sql_rc': (-?\d+)", message)
    if match:
        return int(match.group(1))
    else:
        return None


def test_connect_simple(ibmi_credentials):
    """Test simple SQLJob connection."""
    job = SQLJob()
    result = job.connect(ibmi_credentials)
    assert result["success"]
    job.close()


def test_bad_credentials(ibmi_credentials):
    """Test connection with bad credentials."""
    bad_creds = DaemonServer(
        host=ibmi_credentials["host"],
        port=ibmi_credentials["port"], 
        user="baduser",
        password=ibmi_credentials["password"],
        ignoreUnauthorized=True,
    )

    job = SQLJob()
    result = None
    try:
        result = job.connect(bad_creds)
        raise Exception("error not thrown")
    except Exception as e:
        assert "User ID is not known.:BADUSER" in str(e)
    finally:
        job.close()


def test_simple_query(ibmi_credentials, sample_employee_sql):
    """Test simple query execution."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    query = job.query(sample_employee_sql)
    result = query.run(rows_to_fetch=5)
    assert result["success"]
    job.close()


def test_query_and_run_shortcut(ibmi_credentials, sample_employee_sql):
    """Test query_and_run shortcut method."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    result = job.query_and_run(sample_employee_sql, rows_to_fetch=5)
    assert result["success"]
    job.close()


def test_result_paging(ibmi_credentials):
    """Test paging through large result sets."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=5)
    
    while True:
        assert result["data"] is not None and len(result["data"]) > 0
        if result["is_done"]:
            break
        result = query.fetch_more(rows_to_fetch=5)

    job.close()


def test_sql_error_handling(ibmi_credentials):
    """Test handling of SQL errors."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)

    query = job.query("select * from thisisnotreal")

    try:
        query.run()
    except Exception as e:
        message = str(e)
        assert parse_sql_rc(message) == -204

    job.close()


def test_sql_job_context_manager(ibmi_credentials, simple_count_sql):
    """Test SQLJob with context manager."""
    with SQLJob(ibmi_credentials) as job:
        result = job.query_and_run(simple_count_sql)
        assert result["success"]