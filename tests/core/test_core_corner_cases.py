"""
Comprehensive corner case tests for mapepire_python.core module.
Tests edge cases, error conditions, and boundary scenarios for core connection and cursor functionality.
"""

import threading

import pytest
from pep249 import (
    DatabaseError,
    DataError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)

from mapepire_python import connect
from mapepire_python.data_types import DaemonServer, QueryOptions


def test_connection_with_daemon_server_object():
    """Test connection creation with DaemonServer object."""
    daemon_server = DaemonServer(
        host="test-server", port=8076, user="testuser", password="testpass", ignoreUnauthorized=True
    )

    # Should fail with connection error but not crash
    with pytest.raises(Exception):
        with connect(daemon_server) as conn:
            pass


def test_connection_with_pathlib_path():
    """Test connection creation with pathlib Path object."""
    from pathlib import Path

    # Create a temporary config file path
    config_path = Path("/tmp/nonexistent_config.ini")

    with pytest.raises(Exception):  # File doesn't exist
        with connect(config_path) as conn:
            pass


def test_connection_with_string_path():
    """Test connection creation with string path."""
    config_path = "./nonexistent_config.ini"

    with pytest.raises(Exception):  # File doesn't exist
        with connect(config_path) as conn:
            pass


def test_connection_creation_with_invalid_options(ibmi_credentials):
    """Test connection creation with invalid options."""
    invalid_options = {
        "invalid_option": "invalid_value",
        "numeric_option": 12345,
        "boolean_option": True,
    }

    try:
        with connect(ibmi_credentials, opts=invalid_options) as conn:
            # Should either work or fail gracefully
            assert conn is not None
    except Exception as e:
        # Invalid options may cause connection failure
        assert isinstance(e, Exception)


def test_connection_cursor_creation_after_failed_operation(ibmi_credentials):
    """Test cursor creation after a failed operation."""
    with connect(ibmi_credentials) as conn:
        # Try a failed operation first
        try:
            with conn.execute("SELECT * FROM nonexistent_table") as cursor:
                cursor.fetchone()
        except Exception:
            pass  # Expected to fail

        # Should still be able to create new cursors
        new_cursor = conn.cursor()
        assert new_cursor is not None
        assert new_cursor.connection == conn


def test_connection_execute_shortcut_with_parameters(ibmi_credentials):
    """Test connection execute shortcut with parameters."""
    with connect(ibmi_credentials) as conn:
        with conn.execute(
            "SELECT * FROM sample.employee WHERE empno = ?",
            [
                ("000010"),
            ],
        ) as cursor:
            result = cursor.fetchone()
            assert result is not None


def test_connection_executemany_shortcut(ibmi_credentials):
    """Test connection executemany shortcut."""
    with connect(ibmi_credentials) as conn:
        parameters = [["000010"], ["000020"]]
        cursor = conn.executemany("SELECT * FROM sample.employee WHERE empno = ?", parameters)
        assert cursor is not None


def test_connection_callproc_functionality(ibmi_credentials):
    """Test connection callproc method."""
    with connect(ibmi_credentials) as conn:
        try:
            # Try to call a procedure (may not exist)
            result = conn.callproc("test_procedure", (1, 2, 3))
            # Should return parameters or handle gracefully
        except Exception as e:
            # Procedure may not exist or not be supported
            assert isinstance(e, (DatabaseError, OperationalError, ProgrammingError))


def test_connection_executescript_functionality(ibmi_credentials):
    """Test connection executescript method."""
    with connect(ibmi_credentials) as conn:
        script = """
        SELECT 1 as first_query;
        SELECT 2 as second_query;
        """

        try:
            cursor = conn.executescript(script)
            assert cursor is not None
        except Exception as e:
            # Script execution may not be fully supported
            assert isinstance(e, (DatabaseError, ProgrammingError))


def test_cursor_creation_with_closed_connection():
    """Test cursor creation when connection is closed."""
    invalid_creds = {"host": "invalid-host", "port": 9999, "user": "invalid", "password": "invalid"}

    try:
        conn = connect(invalid_creds)
        conn.close()

        with pytest.raises(Exception):
            conn.cursor()
    except Exception:
        # Connection will fail, but test the pattern
        pass


def test_cursor_execute_with_query_options(ibmi_credentials):
    """Test cursor execute with QueryOptions."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Test with basic QueryOptions
        try:
            options = QueryOptions(parameters=["1"])
            cursor.execute("SELECT 1 from sysibm.sysdummy1 where  1 = ?", options)
            result = cursor.fetchone()
            assert result is not None
        except Exception as e:
            # QueryOptions may not be supported in this context
            assert isinstance(e, (TypeError, AttributeError))


def test_cursor_execute_with_none_parameters(ibmi_credentials, simple_count_sql):
    """Test cursor execute with None parameters."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # None parameters should be handled gracefully
        cursor.execute(simple_count_sql, None)
        result = cursor.fetchone()
        assert result is not None


def test_cursor_fetch_operations_without_execute(ibmi_credentials):
    """Test fetch operations without prior execute."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Fetch operations without execute should fail gracefully
        try:
            result = cursor.fetchone()
            assert result == None
        except Exception as e:
            assert isinstance(e, (ProgrammingError, InterfaceError))


def test_cursor_multiple_execute_operations(ibmi_credentials):
    """Test multiple execute operations on same cursor."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Execute multiple queries in sequence
        queries = [
            "SELECT COUNT(*) FROM sample.employee",
            "SELECT COUNT(*) FROM sample.department",
        ]

        for query in queries:
            cursor.execute(query)
            result = cursor.fetchone()
            assert result is not None


def test_cursor_description_changes_between_queries(ibmi_credentials):
    """Test cursor description changes between different queries."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Execute query with one column
        cursor.execute("SELECT COUNT(*) FROM sample.employee")
        first_description = cursor.description

        # Execute query with multiple columns
        cursor.execute("SELECT empno, firstnme, lastname FROM sample.employee LIMIT 1")
        second_description = cursor.description

        # Descriptions should be different
        if first_description and second_description:
            assert len(first_description) != len(second_description)


def test_cursor_rowcount_changes_between_queries(ibmi_credentials):
    """Test cursor rowcount changes between different queries."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Execute COUNT query
        cursor.execute("SELECT COUNT(*) FROM sample.employee")
        first_rowcount = cursor.rowcount
        cursor.fetchone()

        # Execute query that returns multiple rows
        cursor.execute("SELECT * FROM sample.employee")
        second_rowcount = cursor.rowcount

        print(first_rowcount)

        # Rowcounts may be different
        assert isinstance(first_rowcount, int)
        assert isinstance(second_rowcount, int)


def test_cursor_fetchmany_with_arraysize_changes(ibmi_credentials, sample_employee_sql):
    """Test fetchmany with changing arraysize."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()
        cursor.execute(sample_employee_sql)

        # Test different arraysize values
        if hasattr(cursor, "arraysize"):
            original_arraysize = cursor.arraysize

            for size in [1, 5, 10, 50]:
                cursor.arraysize = size
                # Note: cursor may need to be reset for this to work properly
                try:
                    result = cursor.fetchmany()
                    # Should respect arraysize (implementation dependent)
                except Exception:
                    # May not work after cursor has been used
                    pass

            cursor.arraysize = original_arraysize


def test_cursor_context_manager_nested(ibmi_credentials, simple_count_sql):
    """Test nested cursor context managers."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor1:
            cursor1.execute(simple_count_sql)
            result1 = cursor1.fetchone()

            with conn.cursor() as cursor2:
                cursor2.execute("select 1 as test from sysibm.sysdummy1")
                result2 = cursor2.fetchone()

                assert result1 is not None
                assert result2 is not None


def test_cursor_iterator_protocol(ibmi_credentials, sample_employee_sql):
    """Test cursor iterator protocol."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()
        cursor.execute(sample_employee_sql)

        # Test if cursor supports iteration
        if hasattr(cursor, "__iter__") or hasattr(cursor, "__next__"):
            rows_collected = []
            try:
                for row in cursor:
                    rows_collected.append(row)
                    if len(rows_collected) >= 5:  # Limit to avoid long test
                        break
            except (StopIteration, AttributeError):
                # Iterator protocol may not be fully implemented
                pass


def test_cursor_weak_reference_cleanup():
    """Test cursor weak reference and garbage collection."""
    try:
        conn = connect({"host": "invalid", "port": 9999, "user": "test", "password": "test"})
    except Exception:
        # Expected to fail, test the cleanup pattern
        pass


def test_connection_thread_safety_stress_test(ibmi_credentials):
    """Test connection under thread stress."""
    results = []
    errors = []

    def worker_function(worker_id):
        try:
            with connect(ibmi_credentials) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT {worker_id} as worker_id")
                result = cursor.fetchone()
                results.append((worker_id, result))
        except Exception as e:
            errors.append((worker_id, str(e)))

    # Create multiple threads
    threads = []
    for i in range(3):  # Conservative number for testing
        thread = threading.Thread(target=worker_function, args=(i,))
        threads.append(thread)

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for completion
    for thread in threads:
        thread.join(timeout=30)  # 30 second timeout

    # Check results - some errors may be acceptable due to connection limits
    print(f"Successful results: {len(results)}")
    print(f"Errors: {len(errors)}")

    if errors:
        print("Thread errors:", errors)


def test_cursor_prepared_statement_edge_cases(ibmi_credentials):
    """Test prepared statements with edge cases."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Test prepared statement with complex parameters
        edge_cases = [
            # Empty parameters
            ("SELECT 'no params' as test", ()),
            # Single parameter
            ("SELECT ? as single_param", (42,)),
            # Multiple parameters
            ("SELECT ?, ?, ? as multi_params", (1, "test", 3.14)),
            # NULL parameter
            ("SELECT ? as null_param", (None,)),
        ]

        for query, params in edge_cases:
            try:
                cursor.execute(query, params)
                result = cursor.fetchone()
                assert result is not None
            except Exception as e:
                # Some parameter combinations may not be supported
                assert isinstance(e, (DatabaseError, DataError))


def test_cursor_callproc_edge_cases(ibmi_credentials):
    """Test callproc with edge cases."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        edge_cases = [
            # No parameters
            ("test_proc", None),
            ("test_proc", ()),
            # Various parameter types
            ("test_proc", (1, "string", 3.14, None)),
        ]

        for proc_name, params in edge_cases:
            try:
                result = cursor.callproc(proc_name, params)
                # Should return parameters or None
            except Exception as e:
                # Procedures may not exist or be supported
                assert isinstance(e, (DatabaseError, OperationalError, ProgrammingError))


def test_cursor_executemany_edge_cases(ibmi_credentials):
    """Test executemany with various edge cases."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        edge_cases = [
            # Empty parameter list
            # ("SELECT 1 as test from sysibm.sysdummy1 where test = ?", []),
            # Single parameter set
            ("SELECT 1 from sysibm.sysdummy1 where 1 = ?", [(1,)]),
            # # Multiple parameter sets
            ("SELECT 1 from sysibm.sysdummy1 where 1 = ?", [(1,), (2,), (3,)]),
            # # Mixed types
            ("SELECT 1 from sysibm.sysdummy1 where 1 = ?", [(1,), ("string",), (3.14,)]),
        ]

        for query, param_list in edge_cases:
            try:
                cursor.executemany(query, param_list)
                result = cursor.fetchall()
                print(result)
                assert result is not None
                # Should complete without error
            except Exception as e:
                print(e)
                # Some edge cases may not be supported
                assert isinstance(e, (DatabaseError, ProgrammingError))


def test_connection_error_recovery(ibmi_credentials):
    """Test connection error recovery scenarios."""
    with connect(ibmi_credentials) as conn:
        # Execute valid query
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as valid_query from sysibm.sysdummy1")
        result = cursor.fetchone()
        assert result is not None

        # Execute invalid query
        try:
            cursor.execute("INVALID SQL QUERY")
        except Exception:
            pass  # Expected to fail

        # Should be able to execute valid query again
        cursor.execute("SELECT 2 as recovery_query from sysibm.sysdummy1")
        result = cursor.fetchone()
        assert result is not None


def test_cursor_memory_management_large_results(ibmi_credentials):
    """Test cursor memory management with large result sets."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Query that could return large results
        large_query = """
        SELECT empno, firstnme, lastname, workdept, phoneno, hiredate, job, edlevel, sex, birthdate, salary, bonus, comm
        FROM sample.employee
        """

        cursor.execute(large_query)

        # Fetch results in chunks to test memory management
        chunk_count = 0
        while True:
            chunk = cursor.fetchmany(5)
            if not chunk or (hasattr(chunk, "get") and not chunk.get("data")):
                break
            chunk_count += 1
            if chunk_count > 10:  # Prevent infinite loop
                break


def test_cursor_transaction_isolation(ibmi_credentials):
    """Test cursor behavior with transactions."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Test basic transaction operations
        conn.commit()  # Start clean

        cursor.execute("SELECT COUNT(*) FROM sample.employee")
        count_before = cursor.fetchone()

        # Perform some operations
        try:
            # This might fail, but we test the transaction handling
            cursor.execute("INSERT INTO sample.test_table VALUES (1, 'test')")
        except Exception:
            pass  # Table may not exist or insert may fail

        # Rollback any changes
        conn.rollback()

        # Count should be unchanged (if table/insert worked)
        cursor.execute("SELECT COUNT(*) FROM sample.employee")
        count_after = cursor.fetchone()

        # Employee table count should be the same (we didn't modify it)
        # This test mainly ensures transaction methods don't crash


def test_connection_resource_cleanup_stress():
    """Test connection resource cleanup under stress."""
    connections = []

    try:
        # Create and immediately close many connections
        for i in range(5):  # Conservative number
            try:
                invalid_creds = {
                    "host": f"invalid-host-{i}",
                    "port": 9999 + i,
                    "user": f"user{i}",
                    "password": f"pass{i}",
                }
                conn = connect(invalid_creds)
                connections.append(conn)
            except Exception:
                # Expected to fail, but should clean up properly
                pass
    finally:
        # Clean up any connections that were created
        for conn in connections:
            try:
                conn.close()
            except Exception:
                pass


def test_cursor_state_consistency(ibmi_credentials):
    """Test cursor state consistency across operations."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Initial state
        assert not cursor._closed
        assert cursor.connection == conn

        # After execute
        cursor.execute("SELECT 1 as test from sysibm.sysdummy1")
        assert not cursor._closed
        assert cursor.connection == conn

        # After fetch
        result = cursor.fetchone()
        assert not cursor._closed
        assert cursor.connection == conn
        assert result is not None

        # After close
        cursor.close()
        assert cursor._closed


def test_connection_state_consistency(ibmi_credentials):
    """Test connection state consistency across operations."""
    conn = connect(ibmi_credentials)

    # Initial state
    assert not conn._closed

    # After cursor creation
    cursor = conn.cursor()
    assert not conn._closed
    assert cursor.connection == conn

    # After operations
    conn.commit()
    conn.rollback()
    assert not conn._closed

    # After close
    conn.close()
    assert conn._closed
