import asyncio
import os
import time

import pytest

import mapepire_python
from mapepire_python.asyncio import connect
from mapepire_python.client.async_sql_job import AsyncSQLJob
from mapepire_python.pool.pool_client import Pool, PoolOptions

from .test_setup import *


@pytest.mark.asyncio
async def test_pep249_async_raw():
    async with connect(creds) as conn:
        async with await conn.execute("select * from sample.employee") as cur:
            res = await cur.fetchone()
            assert res is not None


@pytest.mark.asyncio
async def test_pep249_async_next():
    async def async_row_generator(query: str):
        async with connect(creds) as conn:
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

    assert len(rows) > 0


@pytest.mark.asyncio
async def test_pep249_async_for():
    async def async_row_generator(query: str):
        async with connect(creds) as conn:
            async for row in await conn.execute(query):  # type: ignore[misc]
                yield row

    time1 = asyncio.get_event_loop().time()
    rows = []
    async for row in async_row_generator("select * from sample.department"):
        assert row is not None
    time2 = asyncio.get_event_loop().time()

    print(f"time: {time2 - time1}")


def test_sync_pep249():
    start_time = time.time()
    with mapepire_python.connect(creds) as conn:
        with conn.execute("select * from sample.department") as cur:
            cur.fetchall()
    end_time = time.time()
    print(f"Synchronous execution time: {end_time - start_time} seconds")


@pytest.mark.asyncio
async def test_async_pep249():
    start_time = time.time()
    async with connect(creds) as conn:
        async with await conn.execute("select * from sample.department") as cur:
            await cur.fetchall()
    end_time = time.time()
    print(f"Asynchronous execution time: {end_time - start_time} seconds")


@pytest.mark.asyncio
async def test_pool_perf():
    start_time = time.time()
    async with Pool(
        options=PoolOptions(creds=creds, opts=None, max_size=1, starting_size=1)
    ) as pool:
        res = await asyncio.gather(pool.execute("select * from sample.department"))
        assert res[0].success == True

    end_time = time.time()
    print(f"Asynchronous execution time: {end_time - start_time} seconds")


# --- Native async path tests (post to_thread rewrite) ---


@pytest.mark.asyncio
async def test_async_fetchmany():
    async with connect(creds) as conn:
        async with await conn.execute("select * from sample.employee") as cur:
            rows = await cur.fetchmany(5)
            assert len(rows) == 5


@pytest.mark.asyncio
async def test_async_fetchone_multiple():
    async with connect(creds) as conn:
        async with await conn.execute("select * from sample.employee") as cur:
            row1 = await cur.fetchone()
            row2 = await cur.fetchone()
            assert row1 is not None
            assert row2 is not None
            assert row1 != row2


@pytest.mark.asyncio
async def test_async_description():
    async with connect(creds) as conn:
        async with await conn.execute("select * from sample.employee") as cur:
            desc = cur.description
            assert desc is not None
            assert len(desc) > 0
            for col in desc:
                assert len(col) == 7        # PEP 249 7-tuple
                assert isinstance(col[0], str)   # name
                assert col[1] in (str, int, float, bool)  # type_code


@pytest.mark.asyncio
async def test_async_prepared_statement():
    async with connect(creds) as conn:
        cur = await conn.cursor()
        await cur.execute(
            "select * from sample.employee where bonus > ? and job = ?",
            parameters=[500, "PRES"],
        )
        rows = await cur.fetchall()
        assert rows is not None
        assert len(rows) > 0
        await cur.close()


@pytest.mark.asyncio
async def test_async_cursor_explicit():
    async with connect(creds) as conn:
        cur = await conn.cursor()
        await cur.execute("select * from sample.department")
        rows = await cur.fetchall()
        assert rows is not None
        assert len(rows) > 0
        await cur.close()


@pytest.mark.asyncio
async def test_async_executemany():
    params = [
        ["ALICE", "416 111 0001"],
        ["BOB",   "416 111 0002"],
        ["CAROL", "416 111 0003"],
    ]
    async with connect(creds) as conn:
        async with await conn.execute("drop table sample.async_em_test if exists") as cur:
            pass
        async with await conn.execute(
            "CREATE TABLE SAMPLE.async_em_test (name varchar(10), phone varchar(12))"
        ) as cur:
            pass
        cur = await conn.cursor()
        await cur.executemany("INSERT INTO SAMPLE.async_em_test values (?, ?)", params)
        await cur.close()

        async with await conn.execute("select * from sample.async_em_test") as cur:
            rows = await cur.fetchall()
            assert len(rows) == 3


@pytest.mark.asyncio
async def test_async_sql_job_direct():
    async with AsyncSQLJob(creds) as job:
        result = await job.query_and_run("select * from sample.employee")
        assert result.success is True
        assert result.data is not None
        assert len(result.data) > 0
