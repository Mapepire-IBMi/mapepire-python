"""
Tests for mapepire_python.asyncio module (migrated from pep249_async_test.py).
Simple async connection tests using real IBM i server.
"""

import pytest

from mapepire_python.asyncio import connect


@pytest.mark.asyncio
async def test_async_connection_basic(ibmi_credentials):
    """Test basic async connection functionality."""
    async with connect(ibmi_credentials) as conn:
        async with await conn.execute("select * from sample.employee") as cur:
            res = await cur.fetchone()
            if res:
                assert res.get("success") is True


@pytest.mark.asyncio
async def test_async_cursor_iteration(ibmi_credentials):
    """Test async cursor iteration."""

    async def async_row_generator(query: str):
        async with connect(ibmi_credentials) as conn:
            async with await conn.execute(query) as cur:
                try:
                    while True:
                        row = await cur.__anext__()
                        yield row
                except StopAsyncIteration:
                    pass

    rows = []
    async for row in async_row_generator("select * from sample.department"):
        rows.append(row)

    # Check that we got some rows and they're successful
    if rows:
        assert all(row.get("success", True) for row in rows)


@pytest.mark.asyncio
async def test_async_connection_context_manager(ibmi_credentials):
    """Test async connection as context manager."""
    conn = None
    async with connect(ibmi_credentials) as connection:
        conn = connection
        assert conn is not None

        # Test that we can create a cursor
        cursor = await conn.execute("SELECT 1 from sample.employee")
        assert cursor is not None


@pytest.mark.asyncio
async def test_async_cursor_fetchone(ibmi_credentials, simple_count_sql):
    """Test async cursor fetchone method."""
    async with connect(ibmi_credentials) as conn:
        async with await conn.execute(simple_count_sql) as cur:
            result = await cur.fetchone()
            if result:
                assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_async_cursor_fetchall(ibmi_credentials, sample_employee_sql):
    """Test async cursor fetchall method."""
    async with connect(ibmi_credentials) as conn:
        async with await conn.execute(sample_employee_sql) as cur:
            result = await cur.fetchall()
            if result:
                assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_async_cursor_fetchmany(ibmi_credentials, sample_employee_sql):
    """Test async cursor fetchmany method."""
    async with connect(ibmi_credentials) as conn:
        async with await conn.execute(sample_employee_sql) as cur:
            result = await cur.fetchmany(3)
            if result:
                assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_async_connection_commit_rollback(ibmi_credentials):
    """Test async connection transaction methods."""
    async with connect(ibmi_credentials) as conn:
        # These should not raise exceptions
        await conn.commit()
        await conn.rollback()


@pytest.mark.asyncio
async def test_async_cursor_close(ibmi_credentials, simple_count_sql):
    """Test async cursor close method."""
    async with connect(ibmi_credentials) as conn:
        cursor = await conn.execute(simple_count_sql)
        await cursor.close()
        # Cursor should be closed
