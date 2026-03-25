from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Union

from pep249 import aiopep249
from pep249.aiopep249 import ProcArgs, ProcName, QueryParameters, SQLQuery

from ..client.async_sql_job import AsyncSQLJob
from ..data_types import DaemonServer, JobStatus
from .cursor import AsyncCursor


class AsyncConnection(aiopep249.AsyncCursorExecuteMixin, aiopep249.AsyncConnection):
    """
    A DB API 2.0 compliant async connection for Mapepire, as outlined in
    PEP 249.

    Backed by AsyncSQLJob — native async WebSocket I/O, no thread delegation.

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

    def __init__(self, database: Union[DaemonServer, dict, Path], opts: Dict[str, Any] = {}, **kwargs) -> None:
        super().__init__()
        self._job = AsyncSQLJob(options=opts)
        self._database = database
        self._kwargs = kwargs

    async def _ensure_connected(self) -> None:
        if self._job.status == JobStatus.NotStarted:
            await self._job.connect(self._database, **self._kwargs)

    async def cursor(self) -> AsyncCursor:
        await self._ensure_connected()
        return AsyncCursor(self, self._job)

    async def close(self) -> None:
        await self._job.close()

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
        pass

    async def rollback(self) -> None:
        pass
