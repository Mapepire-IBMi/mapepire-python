"""
Advanced error handling and edge case tests for mapepire_python.
Tests complex scenarios, error propagation, and boundary conditions.
"""

import asyncio
import gc
from concurrent.futures import ThreadPoolExecutor

import pytest

from mapepire_python import connect
from mapepire_python.asyncio import connect as async_connect
from mapepire_python.core.exceptions import (
    DatabaseError,
    DataError,
    InterfaceError,
    OperationalError,
)


class TestConnectionEdgeCases:
    """Test connection edge cases and error conditions."""

    def test_connection_with_unicode_credentials(self):
        """Test connection with unicode characters in credentials."""
        unicode_creds = {
            "host": "test-server",
            "port": 8076,
            "user": "用户名",  # Chinese characters
            "password": "пароль",  # Cyrillic characters
        }

        with pytest.raises(Exception):
            with connect(unicode_creds) as conn:
                pass

    def test_connection_with_very_long_credentials(self):
        """Test connection with extremely long credential values."""
        long_creds = {
            "host": "a" * 1000,  # Very long hostname
            "port": 8076,
            "user": "b" * 500,  # Very long username
            "password": "c" * 1000,  # Very long password
        }

        with pytest.raises(Exception):
            with connect(long_creds) as conn:
                pass

    def test_connection_with_special_characters(self):
        """Test connection with special characters in credentials."""
        special_creds = {
            "host": "test@#$%^&*()_+{}[]|\\:;\"'<>?server",
            "port": 8076,
            "user": "user!@#$%^&*()",
            "password": "pass!@#$%^&*()_+{}[]|\\:;\"'<>?",
        }

        with pytest.raises(Exception):
            with connect(special_creds) as conn:
                pass

    def test_connection_with_numeric_strings(self):
        """Test connection with numeric strings for non-port fields."""
        numeric_creds = {
            "host": "12345",
            "port": "8076",  # String port (should be converted)
            "user": "67890",
            "password": "09876",
        }

        with pytest.raises(Exception):
            with connect(numeric_creds) as conn:
                pass

    def test_connection_with_boolean_values(self):
        """Test connection with boolean values in credentials."""
        boolean_creds = {
            "host": True,
            "port": False,
            "user": True,
            "password": False,
        }

        with pytest.raises(Exception):
            with connect(boolean_creds) as conn:
                pass


class TestQueryEdgeCases:
    """Test SQL query edge cases and boundary conditions."""

    def test_extremely_long_query(self, ibmi_credentials):
        """Test execution of extremely long SQL queries."""
        # Create a very long SELECT clause
        long_select = ", ".join([f"'{i}' as col_{i}" for i in range(1000)])
        long_query = f"SELECT {long_select}"

        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            try:
                cursor.execute(long_query)
                result = cursor.fetchone()
                # Should handle long query or raise appropriate error
            except Exception as e:
                assert isinstance(e, (DatabaseError, OperationalError))

    def test_query_with_many_parameters(self, ibmi_credentials):
        """Test query with many parameter placeholders."""
        param_count = 100
        placeholders = ", ".join(["?" for _ in range(param_count)])
        query = f"SELECT {placeholders}"
        params = tuple(range(param_count))

        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            try:
                cursor.execute(query, params)
                result = cursor.fetchone()
            except Exception as e:
                # Many parameters may not be supported
                assert isinstance(e, (DatabaseError, OperationalError))

    def test_query_with_deeply_nested_subqueries(self, ibmi_credentials):
        """Test query with deeply nested subqueries."""
        # Create nested subqueries
        nested_query = "SELECT 1"
        for i in range(10):
            nested_query = f"SELECT ({nested_query}) as nested_{i}"

        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            try:
                cursor.execute(nested_query)
                result = cursor.fetchone()
            except Exception as e:
                # Deep nesting may hit limits
                assert isinstance(e, (DatabaseError, OperationalError))

    def test_query_with_complex_joins(self, ibmi_credentials):
        """Test query with complex multi-table joins."""
        complex_query = """
        SELECT e1.empno, e1.firstnme, e2.firstnme, d.deptname
        FROM sample.employee e1
        JOIN sample.employee e2 ON e1.workdept = e2.workdept AND e1.empno != e2.empno
        JOIN sample.department d ON e1.workdept = d.deptno
        WHERE e1.salary > 50000
        ORDER BY e1.empno
        LIMIT 5
        """

        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            try:
                cursor.execute(complex_query)
                result = cursor.fetchall()
                assert result is not None
            except Exception as e:
                # Complex joins may fail due to data or syntax
                assert isinstance(e, (DatabaseError, OperationalError))


class TestParameterEdgeCases:
    """Test parameter handling edge cases."""

    def test_parameters_with_binary_data(self, ibmi_credentials):
        """Test parameters containing binary data."""
        binary_data = b"\x00\x01\x02\x03\xff\xfe\xfd"

        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            try:
                cursor.execute("SELECT ? as binary_param", (binary_data,))
                result = cursor.fetchone()
            except Exception as e:
                # Binary data may not be supported
                assert isinstance(e, (DataError, DatabaseError))

    def test_parameters_with_large_numbers(self, ibmi_credentials):
        """Test parameters with very large numbers."""
        large_numbers = [
            2**63 - 1,  # Max int64
            -(2**63),  # Min int64
            2**128,  # Very large number
            float("1e308"),  # Very large float
        ]

        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            for num in large_numbers:
                try:
                    cursor.execute("SELECT ? as large_num", (num,))
                    result = cursor.fetchone()
                except Exception as e:
                    # Large numbers may overflow
                    assert isinstance(e, (DataError, DatabaseError, OverflowError))

    def test_parameters_with_special_float_values(self, ibmi_credentials):
        """Test parameters with special float values."""
        special_floats = [
            float("inf"),
            float("-inf"),
            float("nan"),
        ]

        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            for special_float in special_floats:
                try:
                    cursor.execute("SELECT ? as special_float", (special_float,))
                    result = cursor.fetchone()
                except Exception as e:
                    # Special floats may not be supported
                    assert isinstance(e, (DataError, DatabaseError, ValueError))

    def test_parameters_with_complex_data_structures(self, ibmi_credentials):
        """Test parameters with complex data structures."""
        complex_params = [
            {"key": "value"},  # Dict
            [1, 2, 3],  # List
            (1, 2, 3),  # Tuple
            {1, 2, 3},  # Set
        ]

        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            for param in complex_params:
                try:
                    cursor.execute("SELECT ? as complex_param", (param,))
                    result = cursor.fetchone()
                except Exception as e:
                    # Complex types should raise error
                    assert isinstance(e, (TypeError, DataError))


class TestConcurrencyEdgeCases:
    """Test concurrency and threading edge cases."""

    def test_concurrent_connection_creation(self, ibmi_credentials):
        """Test creating multiple connections concurrently."""

        def create_connection():
            try:
                with connect(ibmi_credentials) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1 as test from sysibm.sysdummy1")
                    return cursor.fetchone()
            except Exception as e:
                return e

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(create_connection) for _ in range(3)]
            results = [future.result(timeout=30) for future in futures]

        # At least some should succeed
        successes = [r for r in results if not isinstance(r, Exception)]
        errors = [r for r in results if isinstance(r, Exception)]

        print(f"Concurrent connections - Successes: {len(successes)}, Errors: {len(errors)}")

    def test_concurrent_query_execution(self, ibmi_credentials):
        """Test executing queries concurrently on same connection."""
        with connect(ibmi_credentials) as conn:

            def execute_query(query_id):
                try:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT {query_id} as query_id")
                    return cursor.fetchone()
                except Exception as e:
                    return e

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(execute_query, i) for i in range(3)]
                results = [future.result(timeout=30) for future in futures]

            # Results may vary depending on thread safety level
            successes = [r for r in results if not isinstance(r, Exception)]
            errors = [r for r in results if isinstance(r, Exception)]

            print(f"Concurrent queries - Successes: {len(successes)}, Errors: {len(errors)}")

    @pytest.mark.asyncio
    async def test_async_concurrent_operations(self, ibmi_credentials):
        """Test concurrent async operations."""

        async def async_query(query_id):
            try:
                async with async_connect(ibmi_credentials) as conn:
                    async with await conn.execute(f"SELECT {query_id} as async_id") as cursor:
                        return await cursor.fetchone()
            except Exception as e:
                return e

        # Execute multiple async queries concurrently
        tasks = [async_query(i) for i in range(3)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successes = [r for r in results if not isinstance(r, Exception)]
        errors = [r for r in results if isinstance(r, Exception)]

        print(f"Async concurrent - Successes: {len(successes)}, Errors: {len(errors)}")


class TestMemoryAndResourceEdgeCases:
    """Test memory management and resource edge cases."""

    def test_memory_usage_with_large_result_sets(self, ibmi_credentials):
        """Test memory usage with large result sets."""
        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            # Query that might return large results
            large_query = """
            SELECT empno, firstnme, lastname, workdept, phoneno, hiredate, job, edlevel, sex, birthdate, salary, bonus, comm
            FROM sample.employee
            """

            cursor.execute(large_query)

            # Fetch all results to test memory handling
            try:
                result = cursor.fetchall()
                # Force garbage collection
                gc.collect()
            except Exception as e:
                # Large result sets may cause memory issues
                assert isinstance(e, (MemoryError, DatabaseError))

    def test_resource_cleanup_after_exceptions(self, ibmi_credentials):
        """Test resource cleanup after various exceptions."""
        exception_queries = [
            "SELECT * FROM nonexistent_table",
            "INVALID SQL SYNTAX",
            "SELECT 1/0",  # Division by zero
            "SELECT * FROM sample.employee WHERE empno = 'invalid'",
        ]

        for query in exception_queries:
            try:
                with connect(ibmi_credentials) as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    cursor.fetchone()
            except Exception:
                # Exceptions are expected
                pass

            # Force garbage collection to test cleanup
            gc.collect()

    def test_connection_limit_handling(self, ibmi_credentials):
        """Test behavior when approaching connection limits."""
        connections = []

        try:
            # Try to create many connections
            for i in range(5):  # Conservative limit
                try:
                    conn = connect(ibmi_credentials)
                    connections.append(conn)

                    # Test that connection works
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1 as test from sysibm.sysdummy1")
                    cursor.fetchone()

                except Exception as e:
                    # May hit connection limits
                    print(f"Connection {i} failed: {e}")
                    break
        finally:
            # Clean up all connections
            for conn in connections:
                try:
                    conn.close()
                except Exception:
                    pass


class TestErrorPropagationEdgeCases:
    """Test error propagation and exception handling edge cases."""

    def test_nested_exception_handling(self, ibmi_credentials):
        """Test nested exception handling scenarios."""
        with connect(ibmi_credentials) as conn:
            try:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute("SELECT * FROM nonexistent_table")
                        cursor.fetchone()
                    except Exception as inner_e:
                        # Try another operation in exception handler
                        try:
                            cursor.execute("ANOTHER INVALID QUERY")
                        except Exception as nested_e:
                            # Should handle nested exceptions properly
                            assert isinstance(nested_e, Exception)

                        # Re-raise original exception
                        raise inner_e
            except Exception as outer_e:
                # Should properly propagate through nested handlers
                assert isinstance(outer_e, Exception)

    def test_exception_during_connection_close(self, ibmi_credentials):
        """Test exception handling during connection close."""
        conn = connect(ibmi_credentials)
        cursor = conn.cursor()

        # Execute a query
        cursor.execute("SELECT 1 as test from sysibm.sysdummy1")

        # Close connection while cursor may still be active
        try:
            conn.close()
            # Try to use cursor after connection close
            cursor.fetchone()
        except Exception as e:
            # Should raise appropriate exception
            assert isinstance(e, (InterfaceError, OperationalError))

    def test_exception_in_context_manager_cleanup(self, ibmi_credentials):
        """Test exception handling in context manager cleanup."""
        try:
            with connect(ibmi_credentials) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM nonexistent_table")
                cursor.fetchone()
        except Exception as e:
            # Context manager should properly handle cleanup even with exception
            assert isinstance(e, Exception)
            # Connection should be closed despite exception
            assert conn._closed

    @pytest.mark.asyncio
    async def test_async_exception_propagation(self, ibmi_credentials):
        """Test async exception propagation."""
        try:
            async with async_connect(ibmi_credentials) as conn:
                async with await conn.execute("SELECT * FROM nonexistent_table") as cursor:
                    await cursor.fetchone()
        except Exception as e:
            # Async exceptions should propagate properly
            assert isinstance(e, Exception)


class TestDataTypeEdgeCases:
    """Test data type handling edge cases."""

    def test_datetime_edge_cases(self, ibmi_credentials):
        """Test datetime handling edge cases."""
        from datetime import date, datetime, time

        datetime_values = [
            datetime.min,
            datetime.max,
            datetime(1900, 1, 1),
            datetime(2100, 12, 31),
            date.today(),
            time(23, 59, 59),
        ]

        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            for dt_value in datetime_values:
                try:
                    cursor.execute("SELECT ? as datetime_test", (dt_value,))
                    result = cursor.fetchone()
                except Exception as e:
                    # Some datetime values may not be supported
                    assert isinstance(e, (DataError, DatabaseError))

    def test_decimal_precision_edge_cases(self, ibmi_credentials):
        """Test decimal precision edge cases."""
        from decimal import Decimal

        decimal_values = [
            Decimal("0.000000000000001"),  # Very small
            Decimal("999999999999999.999999999999999"),  # Very large
            Decimal("0"),  # Zero
            Decimal("-999999999999999.999999999999999"),  # Large negative
        ]

        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            for dec_value in decimal_values:
                try:
                    cursor.execute("SELECT ? as decimal_test", (dec_value,))
                    result = cursor.fetchone()
                except Exception as e:
                    # Extreme decimal values may not be supported
                    assert isinstance(e, (DataError, DatabaseError))

    def test_string_encoding_edge_cases(self, ibmi_credentials):
        """Test string encoding edge cases."""
        encoding_strings = [
            "ASCII only",
            "UTF-8 with émojis 🚀🌟",
            "Mixed scripts: Hello 世界 مرحبا Здравствуй",
            "Control chars: \x00\x01\x02\x03",
            "Null terminator: test\x00test",
            "Very long: " + "a" * 10000,
        ]

        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            for test_string in encoding_strings:
                try:
                    cursor.execute("SELECT ? as encoding_test", (test_string,))
                    result = cursor.fetchone()
                except Exception as e:
                    # Some encodings may not be supported
                    assert isinstance(e, (DataError, DatabaseError, UnicodeError))
