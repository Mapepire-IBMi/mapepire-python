"""
Tests for CL command functionality (migrated from cl_test.py).
Simple CL command tests using real IBM i server.
"""

import pytest
from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import QueryOptions


def test_cl_command_successful(ibmi_credentials):
    """Test successful CL command execution."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    opts = QueryOptions(isClCommand=True)
    query = job.query("WRKACTJOB", opts=opts)
    result = query.run()
    job.close()
    assert len(result["data"]) >= 1
    assert result["success"] is True


def test_cl_command_unsuccessful(ibmi_credentials):
    """Test unsuccessful CL command execution.""" 
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    opts = QueryOptions(isClCommand=True)
    query = job.query("INVALIDCOMMAND", opts=opts)
    result = query.run()
    job.close()
    assert len(result["data"]) >= 1
    assert result["success"] is False
    assert "[CPF0006] Errors occurred in command." in result["error"]
    assert result["id"] is not None
    assert result["is_done"] is True
    assert result["sql_rc"] == -443
    assert result["sql_state"] == "38501"


def test_basic_sql_query_in_cl_test(ibmi_credentials):
    """Test basic SQL query functionality."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=5)
    job.close()
    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True