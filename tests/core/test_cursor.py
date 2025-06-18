"""
Tests for mapepire_python.core.cursor module.
Simple tests using real IBM i server.

NOTE: mapepire-python cursor fetch methods return JSON objects instead of tuples.
This is a design choice for IBM i integration and differs from strict PEP 249.
"""

import pytest
from mapepire_python import connect


def test_cursor_creation(ibmi_credentials):
    """Test basic cursor creation."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()
        assert cursor is not None
        assert cursor.connection == conn
        assert not cursor._closed


def test_cursor_execute_simple_query(ibmi_credentials, simple_count_sql):
    """Test executing a simple query."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(simple_count_sql)
            # After execute, cursor should have description if results available
            # For COUNT(*), we expect one column


def test_cursor_fetchone(ibmi_credentials, simple_count_sql):
    """Test fetchone method."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(simple_count_sql)
            row = cursor.fetchone()
            
            assert row is not None
            # mapepire returns JSON objects, not tuples
            assert isinstance(row, dict)
            if row.get("has_results") and row.get("data"):
                assert len(row["data"]) >= 1


def test_cursor_fetchmany(ibmi_credentials, sample_employee_sql):
    """Test fetchmany method."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sample_employee_sql)
            rows = cursor.fetchmany(3)
            
            # mapepire returns JSON objects, not list of tuples
            assert isinstance(rows, dict)
            if rows.get("has_results") and rows.get("data"):
                assert len(rows["data"]) <= 3  # Should return at most 3 rows


def test_cursor_fetchall(ibmi_credentials, sample_employee_sql):
    """Test fetchall method."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sample_employee_sql)
            rows = cursor.fetchall()
            
            # mapepire returns JSON objects, not list of tuples
            assert isinstance(rows, dict)
            if rows.get("has_results") and rows.get("data"):
                assert isinstance(rows["data"], list)


def test_cursor_context_manager(ibmi_credentials, simple_count_sql):
    """Test cursor as context manager."""
    with connect(ibmi_credentials) as conn:
        cursor = None
        with conn.cursor() as cur:
            cursor = cur
            assert not cur._closed
            cur.execute(simple_count_sql)
        
        # Cursor should be closed after context exit
        assert cursor._closed


def test_cursor_multiple_executions(ibmi_credentials):
    """Test multiple executions on same cursor."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            # Execute first query
            cursor.execute("SELECT COUNT(*) FROM sample.employee")
            result1 = cursor.fetchone()
            
            # Execute second query  
            cursor.execute("SELECT COUNT(*) FROM sample.department")
            result2 = cursor.fetchone()
            
            assert result1 is not None
            assert result2 is not None
            # mapepire returns JSON objects
            assert isinstance(result1, dict)
            assert isinstance(result2, dict)


def test_cursor_execute_with_parameters(ibmi_credentials):
    """Test executing query with parameters."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            # Simple parameterized query
            cursor.execute("SELECT ? as test_value", (42,))
            result = cursor.fetchone()
            
            assert result is not None
            # mapepire returns JSON objects
            assert isinstance(result, dict)


def test_cursor_executemany(ibmi_credentials):
    """Test executemany method."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            # Simple test - multiple parameter sets
            parameters = [(1,), (2,), (3,)]
            cursor.executemany("SELECT ? as test_value", parameters)
            # executemany might not return results in the traditional sense
            # This test just ensures it doesn't crash


def test_cursor_rowcount(ibmi_credentials, simple_count_sql):
    """Test rowcount property.""" 
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(simple_count_sql)
            # rowcount behavior may vary by implementation
            # Just ensure it's accessible
            rowcount = cursor.rowcount
            assert isinstance(rowcount, int)


def test_cursor_description(ibmi_credentials, sample_employee_sql):
    """Test description property."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sample_employee_sql)
            description = cursor.description
            
            if description is not None:  # May be None for some query types
                assert isinstance(description, (list, tuple))
                for col_desc in description:
                    assert isinstance(col_desc, (list, tuple))
                    assert len(col_desc) >= 2  # At minimum: name, type


def test_cursor_close_explicit(ibmi_credentials):
    """Test explicit cursor close."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()
        assert not cursor._closed
        
        cursor.close()
        assert cursor._closed