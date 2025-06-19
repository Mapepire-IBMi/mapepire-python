"""
Comprehensive SQL tests (migrated from sql_test.py).
Simple comprehensive SQL tests using real IBM i server.
"""

import pytest

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import QueryOptions


def test_sql_basic_query(ibmi_credentials):
    """Test basic SQL query."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=5)
    job.close()
    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True


def test_sql_dict_credentials(ibmi_credentials):
    """Test SQLJob with dictionary credentials."""
    creds_dict = {
        "host": ibmi_credentials["host"],
        "user": ibmi_credentials["user"],
        "port": ibmi_credentials["port"],
        "password": ibmi_credentials["password"],
        "ignoreUnauthorized": True,
    }
    job = SQLJob()
    _ = job.connect(creds_dict)
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=5)
    job.close()
    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True


def test_sql_context_manager_query_and_run(ibmi_credentials):
    """Test SQLJob context manager with query_and_run."""
    with SQLJob(creds=ibmi_credentials) as job:
        res = job.query_and_run("select * from sample.employee")
        assert res["success"] == True


def test_sql_large_dataset(ibmi_credentials):
    """Test query with large dataset."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=30)
    job.close()

    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True
    assert len(result["data"]) == 30


def test_sql_terse_format(ibmi_credentials):
    """Test query with terse results format."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    opts = QueryOptions(isTerseResults=True)
    query = job.query("select * from sample.employee", opts=opts)
    result = query.run(rows_to_fetch=5)
    job.close()

    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True
    assert (
        "metadata" in result and result["metadata"]
    ), "The 'metadata' key is missing or has no data"


def test_sql_invalid_query(ibmi_credentials):
    """Test invalid query error handling."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    query = job.query("select * from sample.notreal")
    try:
        query.run(rows_to_fetch=5)
        raise Exception("error not raised")
    except Exception as e:
        assert e.args[0]
        assert "error" in e.args[0]
        assert "*FILE not found." in e.args[0]["error"]
    job.close()


def test_sql_empty_query_edge_case(ibmi_credentials):
    """Test empty query edge case."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    query = job.query("")  # empty string
    try:
        _ = query.run(rows_to_fetch=1)
        raise Exception("no error raised")
    except Exception as e:
        assert e.args[0]
        assert "error" in e.args[0]
        assert "A string parameter value with zero length was detected." in e.args[0]["error"]
    finally:
        job.close()


def test_sql_edge_case_inputs(ibmi_credentials):
    """Test various edge case inputs."""
    job = SQLJob()
    job.connect(ibmi_credentials)

    # Test empty string query
    query = job.query("")
    with pytest.raises(Exception) as excinfo:
        query.run(rows_to_fetch=1)
    assert "A string parameter value with zero length was detected." in str(excinfo.value)

    # Test non-string query
    with pytest.raises(Exception) as excinfo:
        query = job.query(666)
        query.run(rows_to_fetch=1)
    assert "Token 666 was not valid" in str(excinfo.value)

    # Test invalid token query
    query = job.query("a")
    with pytest.raises(Exception) as excinfo:
        query.run(rows_to_fetch=1)
    assert "Token A was not valid." in str(excinfo.value)

    # Test long invalid token query
    long_invalid_query = "aeriogfj304tq34projqwe'fa;sdfaSER90Q243RSDASDAFQ#4dsa12$$$YS" * 10
    query = job.query(long_invalid_query)
    with pytest.raises(Exception) as excinfo:
        query.run(rows_to_fetch=1)
    assert "Token AERIOGFJ304TQ34PROJQWE was not valid." in str(excinfo.value)

    # Test valid query with zero rows to fetch
    query = job.query("SELECT * FROM SAMPLE.employee")
    res = query.run(rows_to_fetch=0)
    assert res["data"] == [], "Expected empty result set when rows_to_fetch is 0"

    # Test valid query with non-numeric rows to fetch
    # use default rows_to_fetch == 100
    query = job.query("select * from sample.department")
    res = query.run(rows_to_fetch="s")
    assert res["success"]

    # Test valid query with negative rows to fetch
    query = job.query("select * from sample.department")
    res = query.run(rows_to_fetch=-1)
    assert res["data"] == [], "Expected empty result set when rows_to_fetch < 0"

    job.close()


def test_sql_drop_table(ibmi_credentials):
    """Test DROP TABLE statement."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    query = job.query("drop table sample.delete if exists")
    res = query.run()
    assert res["has_results"] is False
    job.close()


def test_sql_fetch_more(ibmi_credentials):
    """Test fetch_more functionality."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    query = job.query("select * from sample.employee")
    res = query.run(rows_to_fetch=5)
    while not res["is_done"]:
        res = query.fetch_more(10)
        print(res)
        # If no data returned, we're done (handles server correlation ID issues)
        if len(res["data"]) == 0:
            break

    job.close()
    assert res["is_done"]


def test_sql_prepared_statement(ibmi_credentials):
    """Test prepared statement with parameters."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    opts = QueryOptions(parameters=[500])
    query = job.query("select * from sample.employee where bonus > ?", opts=opts)
    res = query.run()
    job.close()
    assert res["success"]
    assert len(res["data"]) >= 17


def test_sql_prepared_statement_terse(ibmi_credentials):
    """Test prepared statement with terse format."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    opts = QueryOptions(parameters=[500], isTerseResults=True)
    query = job.query("select * from sample.employee where bonus > ?", opts=opts)
    res = query.run()
    job.close()
    assert res["success"]
    assert len(res["data"]) >= 17
    assert "metadata" in res


def test_sql_prepared_statement_multiple_params(ibmi_credentials):
    """Test prepared statement with multiple parameters."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    opts = QueryOptions(parameters=[500, "PRES"])
    query = job.query("select * from sample.employee where bonus > ? and job = ?", opts=opts)
    res = query.run()
    assert res["success"]
    job.close()


def test_sql_prepared_statement_invalid_params(ibmi_credentials):
    """Test prepared statement with invalid parameters."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    opts = QueryOptions(parameters=["jjfkdsajf"])
    query = job.query("select * from sample.employee where bonus > ?", opts=opts)
    with pytest.raises(Exception) as execinfo:
        query.run()
    assert "Data type mismatch." in str(execinfo.value)
    job.close()


def test_sql_prepared_statement_no_param(ibmi_credentials):
    """Test prepared statement with missing parameters."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    opts = QueryOptions(parameters=[])
    query = job.query("select * from sample.employee where bonus > ?", opts=opts)
    with pytest.raises(Exception) as execinfo:
        query.run()
    assert (
        "The number of parameter values set or registered does not match the number of parameters."
        in str(execinfo.value)
    )
    job.close()


def test_sql_prepared_statement_too_many_params(ibmi_credentials):
    """Test prepared statement with too many parameters."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    opts = QueryOptions(parameters=[500, "hello"])
    query = job.query("select * from sample.employee where bonus > ?", opts=opts)
    with pytest.raises(Exception) as execinfo:
        query.run()
    assert "Descriptor index not valid. (2>1)" in str(execinfo.value)
    job.close()


def test_sql_prepared_statement_invalid_data_type(ibmi_credentials):
    """Test prepared statement with invalid data type."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    opts = QueryOptions(parameters=[{"bonus": 500}])
    query = job.query("select * from sample.employee where bonus > ?", opts=opts)
    with pytest.raises(Exception) as execinfo:
        query.run()
    assert "JsonObject" in str(execinfo.value)
    job.close()


def test_sql_query_and_run_shortcut(ibmi_credentials):
    """Test query_and_run shortcut method."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)
    res = job.query_and_run("select * from sample.employee")
    job.close()
    assert res["success"]


def test_sql_multiple_statements(ibmi_credentials):
    """Test multiple sequential statements."""
    job = SQLJob()
    _ = job.connect(ibmi_credentials)

    resA = job.query("select * from sample.department").run()
    assert resA["success"] is True

    resB = job.query("select * from sample.employee").run()
    assert resB["success"] is True

    job.close()
