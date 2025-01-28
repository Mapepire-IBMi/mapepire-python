import asyncio
import os
import time

import pytest

import mapepire_python
from mapepire_python.asyncio import connect
from mapepire_python.data_types import DaemonServer
from mapepire_python.pool.pool_client import Pool, PoolOptions

server = os.getenv("VITE_SERVER")
user = os.getenv("VITE_DB_USER")
password = os.getenv("VITE_DB_PASS")
port = os.getenv("VITE_DB_PORT")

# Check if environment variables are set
if not server or not user or not password:
    raise ValueError("One or more environment variables are missing.")


creds = DaemonServer(host=server, port=port, user=user, password=password)


@pytest.mark.asyncio
async def test_pep249_async_raw():
    async with connect(creds) as conn:
        async with await conn.execute("select * from sample.employee") as cur:
            res = await cur.fetchone()
            assert res["success"] == True


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

    assert all(row["success"] for row in rows)


@pytest.mark.asyncio
async def test_pep249_async_for():
    async def async_row_generator(query: str):
        async with connect(creds) as conn:
            async for row in await conn.execute(query):
                yield row

    time1 = asyncio.get_event_loop().time()
    rows = []
    async for row in async_row_generator("select * from sample.department"):
        assert row["success"] == True
    time2 = asyncio.get_event_loop().time()

    print(f"fime: {time2 - time1}")


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
        assert res[0]["success"] == True

    end_time = time.time()
    print(f"Asynchronous execution time: {end_time - start_time} seconds")
