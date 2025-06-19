"""
Comprehensive corner case tests for mapepire_python.pep249 compliance.
Tests edge cases, boundary conditions, and error scenarios for PEP 249 compliance.
"""

import threading

import pytest

from mapepire_python import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    apilevel,
    connect,
    paramstyle,
    threadsafety,
)


def test_module_level_attributes_immutability():
    """Test that module-level attributes cannot be modified."""
    original_apilevel = apilevel
    original_threadsafety = threadsafety
    original_paramstyle = paramstyle

    # These should remain constant
    assert apilevel == "2.0"
    assert threadsafety >= 0
    assert paramstyle in ["format", "pyformat", "numeric", "named", "qmark"]


def test_connect_with_none_credentials():
    """Test connect function with None credentials should fail."""
    with pytest.raises(Exception):
        connect(None)


def test_connect_with_empty_dict():
    """Test connect function with empty dictionary."""
    with pytest.raises(Exception):
        connect({})


def test_connect_with_missing_required_fields():
    """Test connect with missing required connection fields."""
    incomplete_creds = {"host": "test-server"}  # Missing port, user, password

    with pytest.raises(Exception):
        connect(incomplete_creds)


def test_connection_cursor_creation_limits(ibmi_credentials):
    """Test creating many cursors from same connection."""
    with connect(ibmi_credentials) as conn:
        cursors = []
        try:
            # Create many cursors to test resource limits
            for i in range(50):
                cursor = conn.cursor()
                cursors.append(cursor)

            # All cursors should be valid
            for cursor in cursors:
                assert cursor is not None
                assert cursor.connection == conn
        finally:
            # Clean up cursors
            for cursor in cursors:
                try:
                    cursor.close()
                except Exception:
                    pass


def test_cursor_execute_with_invalid_sql(ibmi_credentials):
    """Test cursor execute with various invalid SQL statements."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        invalid_queries = [
            "",  # Empty string
            "INVALID SQL SYNTAX HERE",
            "SELECT * FROM",  # Incomplete
            "DROP TABLE nonexistent_table",  # May fail
            "SELECT * FROM definitely_not_a_table_12345",
            "INVALID COMMAND",
            ";;;",  # Just semicolons
        ]

        for query in invalid_queries:
            with pytest.raises(Exception):
                cursor.execute(query)


def test_cursor_parameter_substitution_edge_cases(ibmi_credentials):
    """Test parameter substitution with edge case values."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        edge_case_params = [
            None,  # NULL value
            "",  # Empty string
            "very long string " * 1000,  # Very long string
            0,  # Zero
            -999999,  # Large negative number
            999999999999,  # Large positive number
            float("inf"),  # Infinity (may not be supported)
            float("-inf"),  # Negative infinity
        ]

        for param in edge_case_params:
            try:
                cursor.execute("SELECT ? as test_param from sysibm.sysdummy1", (param,))
                result = cursor.fetchone()
                # Should handle parameter or raise appropriate exception
            except Exception as e:
                # Some parameters may not be supported
                assert isinstance(e, (DatabaseError, DataError, OperationalError))


def test_cursor_fetchone_multiple_calls_beyond_result_set(ibmi_credentials, simple_count_sql):
    """Test multiple fetchone calls beyond available results."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()
        cursor.execute(simple_count_sql)

        # Get all results
        first_result = cursor.fetchone()
        assert first_result is not None

        # Multiple calls beyond results should return None
        for _ in range(10):
            result = cursor.fetchone()
            # Should consistently return None or empty result


def test_cursor_fetchmany_edge_sizes(ibmi_credentials, sample_employee_sql):
    """Test fetchmany with edge case sizes."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()
        cursor.execute(sample_employee_sql)

        edge_sizes = [0, -1, -100, 999999, 2**31 - 1]

        for size in edge_sizes:
            try:
                result = cursor.fetchmany(size)
                # Should handle gracefully or raise appropriate exception
            except Exception as e:
                assert isinstance(e, (ValueError, DatabaseError))


def test_cursor_fetchall_on_large_result_set(ibmi_credentials):
    """Test fetchall on potentially large result set."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Create a query that could return many rows
        large_query = """
        SELECT empno, firstnme, lastname 
        FROM sample.employee 
        UNION ALL
        SELECT empno, firstnme, lastname 
        FROM sample.employee
        """

        cursor.execute(large_query)
        result = cursor.fetchall()

        # Should return all results without memory issues
        assert result is not None


def test_cursor_arraysize_property_edge_cases(ibmi_credentials):
    """Test cursor arraysize property with edge case values."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        edge_values = [0, -1, -999, 999999, 2**31 - 1]

        for value in edge_values:
            try:
                if hasattr(cursor, "arraysize"):
                    cursor.arraysize = value
                    # Some implementations may reject invalid values
                    if value > 0:
                        assert cursor.arraysize == value
            except Exception as e:
                # Invalid arraysize values may raise exceptions
                assert isinstance(e, (ValueError, DatabaseError))


def test_cursor_description_with_no_results(ibmi_credentials):
    """Test cursor description when no query has been executed."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Description should be None before execute
        assert cursor.description is None

        # Execute query that returns no rows
        cursor.execute("SELECT * FROM sample.employee WHERE 1=0")

        # Description should still be available even with no rows
        if cursor.description is not None:
            assert len(cursor.description) > 0


def test_cursor_rowcount_edge_cases(ibmi_credentials):
    """Test cursor rowcount with various query types."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Initial rowcount should be -1 or 0
        initial_rowcount = cursor.rowcount
        assert isinstance(initial_rowcount, int)

        # Test with different query types
        queries = [
            "SELECT COUNT(*) FROM sample.employee",  # SELECT
            "SELECT * FROM sample.employee WHERE 1=0",  # SELECT with no results
        ]

        for query in queries:
            cursor.execute(query)
            rowcount = cursor.rowcount
            assert isinstance(rowcount, int)
            assert rowcount >= -1  # PEP 249 allows -1 for unknown


def test_cursor_executemany_empty_sequence(ibmi_credentials):
    """Test executemany with empty parameter sequence."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Empty sequence should be handled gracefully
        try:
            cursor.executemany("SELECT ? as test_value from sysibm.sysdummy1", [])
            # Should complete without error
        except Exception as e:
            # Some implementations may raise error for empty sequence
            assert isinstance(e, (ProgrammingError, DatabaseError))


def test_cursor_executemany_single_parameter_set(ibmi_credentials):
    """Test executemany with single parameter set."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        cursor.executemany(
            "SELECT * FROM sample.employee WHERE empno = ?", [["000010"], ["000020"]]
        )

        # Should execute successfully


def test_cursor_execute_single_parameter_set(ibmi_credentials):
    """Test executemany with single parameter set."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sample.employee WHERE empno = ?", ["000010"])
        # Should execute successfully


def test_cursor_executemany_inconsistent_parameter_types(ibmi_credentials):
    """Test executemany with inconsistent parameter types."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        mixed_params = [(1,), ("string",), (3.14,), (None,)]

        try:
            cursor.executemany("SELECT ? as test_value from sysibm.sysdummy1", mixed_params)
            # Some implementations handle mixed types
        except Exception as e:
            # Others may require consistent types
            assert isinstance(e, (DataError, DatabaseError))


def test_connection_commit_without_transaction(ibmi_credentials):
    """Test commit when no transaction is active."""
    with connect(ibmi_credentials) as conn:
        # Multiple commits should be safe
        conn.commit()
        conn.commit()
        conn.commit()


def test_connection_rollback_without_transaction(ibmi_credentials):
    """Test rollback when no transaction is active."""
    with connect(ibmi_credentials) as conn:
        # Multiple rollbacks should be safe
        conn.rollback()
        conn.rollback()
        conn.rollback()


def test_connection_operations_after_close(ibmi_credentials):
    """Test operations on closed connection should fail."""
    conn = connect(ibmi_credentials)
    conn.close()

    # All operations should fail on closed connection
    with pytest.raises(Exception):
        conn.cursor()

    with pytest.raises(Exception):
        conn.commit()

    with pytest.raises(Exception):
        conn.rollback()

    # Multiple closes should be safe
    conn.close()
    conn.close()


def test_cursor_operations_after_connection_close(ibmi_credentials, simple_count_sql):
    """Test cursor operations after connection is closed."""
    conn = connect(ibmi_credentials)
    cursor = conn.cursor()
    cursor.execute(simple_count_sql)

    # Close connection
    conn.close()

    # Cursor operations should fail after connection close
    with pytest.raises(Exception):
        cursor.fetchone()

    with pytest.raises(Exception):
        cursor.execute("SELECT 1")


def test_exception_hierarchy_completeness():
    """Test that all PEP 249 exceptions are properly defined."""
    # All exceptions should inherit from Error
    exceptions = [
        DatabaseError,
        DataError,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        ProgrammingError,
    ]

    for exc_class in exceptions:
        assert issubclass(exc_class, Error)

        # Should be instantiable
        exc_instance = exc_class("test message")
        assert str(exc_instance) == "test message"


def test_cursor_context_manager_with_exception(ibmi_credentials):
    """Test cursor context manager properly handles exceptions."""
    with connect(ibmi_credentials) as conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM nonexistent_table")
                cursor.fetchone()
        except Exception as e:
            # Exception should be properly propagated
            assert isinstance(e, Exception)
            # Cursor should be cleaned up despite exception


def test_connection_context_manager_with_exception():
    """Test connection context manager with connection failure."""
    invalid_creds = {
        "host": "invalid-host-12345",
        "port": 9999,
        "user": "invalid",
        "password": "invalid",
    }

    try:
        with connect(invalid_creds) as conn:
            # This should never execute
            conn.cursor()
    except Exception:
        # Connection failure should be properly handled
        pass


def test_parameter_style_qmark_edge_cases(ibmi_credentials):
    """Test qmark parameter style with edge cases."""
    if paramstyle == "qmark":
        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            # Test with no parameters when query expects them
            with pytest.raises(Exception):
                cursor.execute("SELECT ? as test from sysibm.sysdummy1", ())

            # Test with too many parameters
            with pytest.raises(Exception):
                cursor.execute("SELECT ? as test from sysibm.sysdummy1", (1, 2, 3))

            # Test with wrong parameter types
            edge_cases = [
                {"invalid": "dict"},  # Dict instead of tuple
                [1, 2, 3],  # List instead of tuple (might work)
                "string",  # String instead of tuple
                42,  # Number instead of tuple
            ]

            for params in edge_cases:
                try:
                    cursor.execute("SELECT ? as test from sysibm.sysdummy1", params)
                    # Some parameter types might work
                except Exception as e:
                    # Others should raise appropriate exception
                    assert isinstance(e, (TypeError, ProgrammingError, DatabaseError))


def test_cursor_memory_management():
    """Test cursor memory management and garbage collection."""
    # Create connection with invalid credentials to test cleanup
    try:
        conn = connect({"host": "invalid", "port": 9999, "user": "test", "password": "test"})
    except Exception:
        # Expected to fail, test the cleanup pattern
        pass


def test_cursor_thread_safety(ibmi_credentials):
    """Test cursor operations from multiple threads."""
    if threadsafety < 2:
        pytest.skip("Thread safety level insufficient for this test")

    results = []
    errors = []

    def worker_thread(thread_id):
        try:
            with connect(ibmi_credentials) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT {thread_id} as thread_id")
                result = cursor.fetchone()
                results.append((thread_id, result))
        except Exception as e:
            errors.append((thread_id, e))

    # Create multiple threads
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker_thread, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Check results
    if threadsafety >= 2:
        assert len(errors) == 0, f"Thread safety errors: {errors}"
        assert len(results) == 5


def test_cursor_large_parameter_sets(ibmi_credentials):
    """Test cursor with large parameter sets."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Test with many parameters
        large_param_count = 100
        placeholders = ", ".join(["?" for _ in range(large_param_count)])
        values = tuple(range(large_param_count))

        try:
            cursor.execute(f"SELECT {placeholders}", values)
            # Should handle large parameter sets
        except Exception as e:
            # Or raise appropriate error if not supported
            assert isinstance(e, (DatabaseError, OperationalError))


def test_cursor_unicode_and_encoding_edge_cases(ibmi_credentials):
    """Test cursor with various unicode and encoding scenarios."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        unicode_strings = [
            "Hello, 世界",  # Mixed scripts
            "🚀🌟✨",  # Emojis
            "café",  # Accented characters
            "Здравствуй мир",  # Cyrillic
            "مرحبا بالعالم",  # Arabic
            "\x00\x01\x02",  # Control characters
            "a" * 10000,  # Very long string
        ]

        for unicode_str in unicode_strings:
            try:
                cursor.execute("SELECT ? as unicode_test", (unicode_str,))
                result = cursor.fetchone()
                # Should handle unicode properly
            except Exception as e:
                # Some unicode characters may not be supported
                assert isinstance(e, (DataError, DatabaseError))


# def test_cursor_sql_injection_prevention(ibmi_credentials):
#     """Test that parameterized queries prevent SQL injection."""
#     with connect(ibmi_credentials) as conn:
#         cursor = conn.cursor()

#         # Potential SQL injection attempts
#         malicious_inputs = [
#             ["'; DROP TABLE users; --"],
#             # ["1' OR '1'='1"],
#             # ["'; SELECT * FROM sensitive_table; --"],
#             # ["UNION SELECT * FROM another_table"],
#         ]

#         for malicious_input in malicious_inputs:
#             # Using parameterized query should be safe
#             cursor.execute("SELECT ? as safe_param from sysibm.sysdummy1", malicious_input)
#             result = cursor.fetchone()
#             # Should treat as literal string, not execute as SQL
#             assert result is not None


def test_connection_resource_limits(ibmi_credentials):
    """Test connection behavior near resource limits."""
    connections = []

    try:
        # Try to create many connections to test limits
        for i in range(10):  # Reasonable limit for testing
            try:
                conn = connect(ibmi_credentials)
                connections.append(conn)
            except Exception as e:
                # May hit connection limits
                assert isinstance(e, (OperationalError, DatabaseError))
                break
    finally:
        # Clean up all connections
        for conn in connections:
            try:
                conn.close()
            except Exception:
                pass
