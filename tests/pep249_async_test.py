import os

import pytest

from mapepire_python.asyncio import connect
from mapepire_python.data_types import DaemonServer

server = os.getenv("VITE_SERVER")
user = os.getenv("VITE_DB_USER")
password = os.getenv("VITE_DB_PASS")
port = os.getenv("VITE_DB_PORT")

# Check if environment variables are set
if not server or not user or not password:
    raise ValueError("One or more environment variables are missing.")


creds = DaemonServer(
    host=server,
    port=port,
    user=user,
    password=password,
    ignoreUnauthorized=True,
)


@pytest.mark.asyncio
async def test_pep249_async_raw():
    async with connect(creds) as conn:
        async with await conn.execute("select * from sample.employee") as cur:
            print(await cur.fetchone())


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

    async for row in async_row_generator("select * from sample.employee"):
        print(row)


@pytest.mark.asyncio
async def test_pep249_async_for():
    async def async_row_generator(query: str):
        async with connect(creds) as conn:
            async for row in await conn.execute(query):
                yield row

    async for row in async_row_generator("select * from sample.employee"):
        print(row)
