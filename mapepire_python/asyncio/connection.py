from pathlib import Path
from typing import Optional, Sequence, Union

from pep249 import aiopep249
from pep249.aiopep249 import ProcArgs, ProcName, QueryParameters, SQLQuery

from ..core.connection import Connection
from ..data_types import DaemonServer
from .cursor import AsyncCursor
from .utils import to_thread


class AsyncConnection(aiopep249.AsyncCursorExecuteMixin, aiopep249.AsyncConnection):
    """
    A DB API 2.0 compliant async connection for Mapepire, as outlined in
    PEP 249.

    Can be constructed by passing a connection details object as a dict,
    or a `DaemonServer` object:

    ```
    import asyncio
    from mapepire_python.asyncio import connect
    from mapepire_python.data_types import DaemonServer
    creds = DaemonServer(
        host=SERVER,
        port=PORT,
        user=USER,
        password=PASS,
        ignoreUnauthorized=True
    )

    >>> async def main():
    ...    async with connect(creds) as conn:
    ...        async with await conn.execute("select * from sample.employee") as cur:
    ...            print(await cur.fetchone())

    >>> if __name__ == '__main__':
    ...     asyncio.run(main())

    ```


    """

    def __init__(self, database: Union[DaemonServer, dict, Path], opts={}, **kwargs) -> None:
        super().__init__()
        self._connection = Connection(database, opts=opts, **kwargs)

    async def cursor(self) -> AsyncCursor:
        return AsyncCursor(self, self._connection.cursor())

    async def close(self) -> None:
        await to_thread(self._connection.close)

    async def execute(
        self, operation: SQLQuery, parameters: Optional[QueryParameters] = None
    ) -> AsyncCursor:
        cursor = await self.cursor()
        return await cursor.execute(operation, parameters)

    async def executemany(
        self, operation: SQLQuery, seq_of_parameters: Sequence[QueryParameters]
    ) -> AsyncCursor:
        cursor = await self.cursor()
        return await cursor.executemany(operation, seq_of_parameters)

    async def callproc(
        self, procname: ProcName, parameters: Optional[ProcArgs] = None
    ) -> Optional[ProcArgs]:
        cursor = await self.cursor()
        return await cursor.callproc(procname, parameters)

    async def executescript(self, script: SQLQuery) -> AsyncCursor:
        """A lazy implementation of SQLite's `executescript`."""
        return await self.execute(script)

    async def commit(self) -> None:
        await to_thread(self._connection.commit)

    async def rollback(self) -> None:
        await to_thread(self._connection.rollback)
