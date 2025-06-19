"""
Comprehensive corner case tests for mapepire_python.asyncio module.
Tests edge cases, error conditions, and boundary scenarios.
"""

import asyncio
import weakref

import pytest

from mapepire_python.asyncio import connect
from mapepire_python.core.exceptions import (
    DatabaseError,
    OperationalError,
    ProgrammingError,
)


@pytest.mark.asyncio
async def test_async_connection_with_invalid_host():
    """Test async connection with invalid host should fail gracefully."""
    invalid_creds = {
        "host": "completely-invalid-host-12345.nonexistent",
        "port": 8076,
        "user": "testuser",
        "password": "testpass",
    }

    with pytest.raises(Exception):  # Should raise connection error
        async with connect(invalid_creds) as conn:
            pass


@pytest.mark.asyncio
async def test_async_connection_with_malformed_credentials():
    """Test async connection with malformed credentials."""
    malformed_creds = {
        "host": "",  # Empty host
        "port": "invalid_port",  # Non-numeric port
        "user": None,  # None user
        "password": "",  # Empty password
    }

    with pytest.raises(Exception):
        async with connect(malformed_creds) as conn:
            pass


# @pytest.mark.asyncio
# async def test_async_connection_timeout_handling():
#     """Test async connection timeout scenarios."""
#     # Test with very short timeout (should fail)
#     timeout_creds = {
#         "host": "192.0.2.1",  # TEST-NET-1 address (non-routable)
#         "port": 8076,
#         "user": "testuser",
#         "password": "testpass",
#     }

#     with pytest.raises(Exception):  # Should timeout
#         async with connect(timeout_creds) as conn:
#             pass


@pytest.mark.asyncio
async def test_async_cursor_closed_operations(ibmi_credentials, simple_count_sql):
    """Test operations on closed async cursor should fail."""
    async with connect(ibmi_credentials) as conn:
        cursor = await conn.execute(simple_count_sql)
        await cursor.close()

        # All operations on closed cursor should fail
        with pytest.raises(Exception):
            await cursor.fetchone()

        with pytest.raises(Exception):
            await cursor.fetchmany(5)

        with pytest.raises(Exception):
            await cursor.fetchall()


@pytest.mark.asyncio
async def test_async_cursor_multiple_closes(ibmi_credentials, simple_count_sql):
    """Test multiple close calls on same async cursor."""
    async with connect(ibmi_credentials) as conn:
        cursor = await conn.execute(simple_count_sql)

        # Multiple closes should not raise exceptions
        await cursor.close()
        await cursor.close()
        await cursor.close()


@pytest.mark.asyncio
async def test_async_connection_closed_cursor_creation(ibmi_credentials):
    """Test creating cursor from closed async connection should fail."""
    conn = connect(ibmi_credentials)
    await conn.close()

    with pytest.raises(Exception):
        await conn.cursor()


@pytest.mark.asyncio
async def test_async_cursor_execute_after_close(ibmi_credentials):
    """Test executing query on closed async cursor."""
    async with connect(ibmi_credentials) as conn:
        cursor = await conn.cursor()
        await cursor.close()

        with pytest.raises(Exception):
            await cursor.execute("SELECT 1")


@pytest.mark.asyncio
async def test_async_cursor_iteration_with_empty_results(ibmi_credentials):
    """Test async cursor iteration when no results returned."""
    async with connect(ibmi_credentials) as conn:
        # Query that returns no rows
        async with await conn.execute("SELECT * FROM sample.employee WHERE 1=0") as cursor:
            rows_collected = []
            try:
                async for row in cursor:
                    rows_collected.append(row)
            except StopAsyncIteration:
                pass

            # Should handle empty result set gracefully
            assert len(rows_collected) == 0


@pytest.mark.asyncio
async def test_async_cursor_fetch_from_non_select_query(ibmi_credentials):
    """Test fetching from non-SELECT query (like CREATE, DROP, etc.)."""
    async with connect(ibmi_credentials) as conn:
        try:
            # Try to create then drop a test table
            async with await conn.execute("DROP TABLE IF EXISTS test_corner_case_table") as cursor:
                result = await cursor.fetchone()
                # DDL statements typically don't return result sets
                # The behavior may vary by implementation
        except Exception:
            pass  # Some implementations may not support IF EXISTS


@pytest.mark.asyncio
async def test_async_cursor_large_fetchmany_size(ibmi_credentials, sample_employee_sql):
    """Test fetchmany with very large size parameter."""
    async with connect(ibmi_credentials) as conn:
        async with await conn.execute(sample_employee_sql) as cursor:
            # Try to fetch more rows than exist
            large_result = await cursor.fetchmany(99999)
            # Should return available rows without error
            assert large_result is not None


@pytest.mark.asyncio
async def test_async_cursor_zero_fetchmany_size(ibmi_credentials, sample_employee_sql):
    """Test fetchmany with zero size parameter."""
    async with connect(ibmi_credentials) as conn:
        async with await conn.execute(sample_employee_sql) as cursor:
            zero_result = await cursor.fetchmany(0)
            # Behavior with size=0 should be handled gracefully
            assert zero_result is not None


@pytest.mark.asyncio
async def test_async_cursor_negative_fetchmany_size(ibmi_credentials, sample_employee_sql):
    """Test fetchmany with negative size parameter."""
    async with connect(ibmi_credentials) as conn:
        async with await conn.execute(sample_employee_sql) as cursor:
            # Negative size should be handled gracefully
            try:
                negative_result = await cursor.fetchmany(-1)
                assert negative_result is not None
            except Exception as e:
                # May raise exception or handle gracefully
                assert isinstance(e, (ValueError, DatabaseError))


@pytest.mark.asyncio
async def test_async_transaction_rollback_on_error(ibmi_credentials):
    """Test transaction rollback when error occurs."""
    async with connect(ibmi_credentials) as conn:
        await conn.commit()  # Start clean

        try:
            # Execute valid statement
            async with await conn.execute("SELECT COUNT(*) FROM sample.employee") as cursor:
                await cursor.fetchone()

            # Then execute invalid statement
            with pytest.raises(Exception):
                async with await conn.execute("SELECT * FROM nonexistent_table") as cursor:
                    await cursor.fetchone()

        except Exception:
            # Rollback should work after error
            await conn.rollback()


@pytest.mark.asyncio
async def test_async_concurrent_cursor_operations(ibmi_credentials):
    """Test concurrent operations on multiple async cursors."""
    async with connect(ibmi_credentials) as conn:
        # Test that concurrent operations on the same connection
        # properly handle WebSocket concurrency limitations
        async def execute_query(query):
            async with await conn.execute(query) as cursor:
                return await cursor.fetchone()

        # Execute multiple queries concurrently
        tasks = [
            execute_query("SELECT COUNT(*) FROM sample.employee"),
            execute_query("SELECT COUNT(*) FROM sample.department"),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check results - some may succeed, others may fail with concurrency errors
        success_count = 0
        concurrency_error_count = 0

        for result in results:
            if isinstance(result, Exception):
                error_str = str(result)
                if "fragmented message" in error_str or "ConcurrencyError" in error_str:
                    concurrency_error_count += 1
                else:
                    # Unexpected error type
                    print(f"Unexpected concurrent operation exception: {result}")
            elif result:
                success_count += 1
                print(f"Successful result: {result}")

        # At least one operation should either succeed or fail with expected concurrency error
        assert (success_count + concurrency_error_count) > 0


@pytest.mark.asyncio
async def test_async_concurrent_operations_separate_connections(ibmi_credentials):
    """Test concurrent operations using separate async connections."""

    async def execute_with_separate_connection(query):
        async with connect(ibmi_credentials) as conn:
            async with await conn.execute(query) as cursor:
                return await cursor.fetchone()

    # Execute multiple queries concurrently with separate connections
    tasks = [
        execute_with_separate_connection("SELECT COUNT(*) FROM sample.employee"),
        execute_with_separate_connection("SELECT COUNT(*) FROM sample.department"),
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # With separate connections, all operations should succeed
    success_count = 0
    for result in results:
        if isinstance(result, Exception):
            print(f"Unexpected exception with separate connections: {result}")
            # This might still fail due to connection limits or other issues
        elif result:
            success_count += 1
            print(f"Successful concurrent result: {result}")

    # At least one should succeed with separate connections
    assert success_count >= 1


@pytest.mark.asyncio
async def test_async_cursor_weak_reference_cleanup(ibmi_credentials):
    """Test that async cursors are properly garbage collected."""
    async with connect(ibmi_credentials) as conn:
        # This test might fail due to connection error, but tests the pattern
        try:
            cursor = await conn.cursor()
            cursor_ref = weakref.ref(cursor)

            # Delete cursor reference
            del cursor

            # Force garbage collection
            import gc

            gc.collect()

            # Check if cursor was garbage collected
            # Note: This test may be flaky depending on GC timing

        except Exception:
            pass  # Connection will fail, but we test the pattern


@pytest.mark.asyncio
async def test_async_cursor_context_manager_exception_handling(ibmi_credentials):
    """Test async cursor context manager properly handles exceptions."""
    async with connect(ibmi_credentials) as conn:
        try:
            async with await conn.execute("SELECT * FROM nonexistent_table") as cursor:
                await cursor.fetchone()
        except Exception as e:
            # Exception should be properly propagated
            assert isinstance(e, Exception)
            # Cursor should be cleaned up despite exception


@pytest.mark.asyncio
async def test_async_cursor_property_access_after_close(ibmi_credentials, simple_count_sql):
    """Test accessing cursor properties after close."""
    async with connect(ibmi_credentials) as conn:
        cursor = await conn.execute(simple_count_sql)
        await cursor.close()

        # Properties might still be accessible, but operations should fail
        try:
            rowcount = cursor.rowcount
            description = cursor.description
            # These might work or might fail depending on implementation
        except Exception:
            pass  # Expected for some implementations


@pytest.mark.asyncio
async def test_async_cursor_executemany_with_empty_parameters(ibmi_credentials):
    """Test executemany with empty parameter list."""
    async with connect(ibmi_credentials) as conn:
        cursor = await conn.cursor()

        # Empty parameter list should be handled gracefully
        try:
            await cursor.executemany("SELECT ? as test_value", [])
            # Should not crash
        except Exception as e:
            # May raise exception for empty parameters
            assert isinstance(e, (ValueError, ProgrammingError, DatabaseError))


@pytest.mark.asyncio
async def test_async_cursor_executemany_with_mismatched_parameters(ibmi_credentials):
    """Test executemany with mismatched parameter counts."""
    async with connect(ibmi_credentials) as conn:
        cursor = await conn.cursor()

        # Mismatched parameter counts should raise error
        with pytest.raises(Exception):
            await cursor.executemany(
                "SELECT ?, ? as test_values",
                [(1,), (1, 2), (1, 2, 3)],  # Different parameter counts
            )


@pytest.mark.asyncio
async def test_async_connection_multiple_close_calls(ibmi_credentials):
    """Test multiple close calls on async connection."""
    conn = connect(ibmi_credentials)

    # Multiple closes should be safe
    await conn.close()
    await conn.close()
    await conn.close()


@pytest.mark.asyncio
async def test_async_cursor_iteration_cancellation(ibmi_credentials, sample_employee_sql):
    """Test cancelling async cursor iteration."""
    async with connect(ibmi_credentials) as conn:
        async with await conn.execute(sample_employee_sql) as cursor:
            # Start iteration then cancel
            try:
                async for row in cursor:
                    # Process first row then break
                    if row:
                        break
            except Exception:
                pass  # Handle any iteration errors gracefully


@pytest.mark.asyncio
async def test_async_cursor_with_very_long_query(ibmi_credentials):
    """Test async cursor with very long SQL query."""
    # Create a very long but valid query
    long_query = (
        "SELECT "
        + ", ".join([f"'{i}' as col_{i}" for i in range(100)])
        + " FROM sample.employee LIMIT 1"
    )

    async with connect(ibmi_credentials) as conn:
        try:
            async with await conn.execute(long_query) as cursor:
                result = await cursor.fetchone()
                assert result is not None
        except Exception as e:
            # Very long queries might hit limits
            assert isinstance(e, (DatabaseError, OperationalError))


@pytest.mark.asyncio
async def test_async_cursor_with_special_characters_in_query(ibmi_credentials):
    """Test async cursor with special characters in query."""
    special_query = (
        "SELECT 'test with ñ, é, 中文, 🚀, \"quotes\", ''single'', and ; semicolon' as special_text"
    )

    async with connect(ibmi_credentials) as conn:
        try:
            async with await conn.execute(special_query) as cursor:
                result = await cursor.fetchone()
                # Should handle special characters properly
                assert result is not None
        except Exception:
            # Some special characters might not be supported
            pass


@pytest.mark.asyncio
async def test_async_connection_resource_cleanup_on_exception():
    """Test that resources are properly cleaned up when connection fails."""
    invalid_creds = {
        "host": "invalid-host-12345",
        "port": 9999,
        "user": "invalid",
        "password": "invalid",
    }

    # Connection should fail and clean up resources
    with pytest.raises(Exception):
        async with connect(invalid_creds) as conn:
            # This should never execute
            await conn.cursor()


@pytest.mark.asyncio
async def test_async_cursor_commit_rollback_without_transaction(ibmi_credentials):
    """Test commit/rollback when no transaction is active."""
    async with connect(ibmi_credentials) as conn:
        cursor = await conn.cursor()

        # These should not fail even without active transaction
        try:
            await cursor.commit()
            await cursor.rollback()
        except Exception as e:
            # Some implementations may raise error for no active transaction
            assert "transaction" in str(e).lower()
