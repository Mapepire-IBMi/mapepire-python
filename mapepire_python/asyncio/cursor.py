import logging
import weakref
from typing import TYPE_CHECKING, List, Optional, Sequence, Type, Union

from pep249 import QueryParameters, ResultRow, aiopep249
from pep249.aiopep249.types import (
    ColumnDescription,
    ProcArgs,
    ProcName,
    ResultSet,
    SQLQuery,
)

if TYPE_CHECKING:
    from .connection import AsyncConnection

from ..client.async_sql_job import AsyncSQLJob
from ..core.utils import DB_TYPE_MAP, row_to_tuple
from ..data_types import QueryOptions
from ..pool.pool_query import PoolQuery

logger = logging.getLogger(__name__)


class AsyncCursor(
    aiopep249.CursorConnectionMixin,
    aiopep249.IterableAsyncCursorMixin,
    aiopep249.TransactionalAsyncCursor,
):
    """
    An async DB API 2.0 compliant cursor for Mapepire, as outlined in
    PEP 249.

    Backed by AsyncSQLJob and PoolQuery — native async WebSocket I/O,
    no thread delegation.
    """

    def __init__(self, connection: "AsyncConnection", job: AsyncSQLJob) -> None:
        super().__init__()
        self._connection = weakref.proxy(connection)
        self._job = job
        self._query: Optional[PoolQuery] = None
        self._buffer: List = []
        self._is_done: bool = True
        self._metadata = None
        self._rowcount: int = -1

    @property
    def connection(self) -> "AsyncConnection":  # type: ignore
        return self._connection

    @property
    def description(self) -> Optional[Sequence[ColumnDescription]]:
        if not self._metadata or not self._metadata.columns:
            return None
        return [
            (
                col.name,
                DB_TYPE_MAP.get(col.type.upper() if col.type else "", str),
                col.display_size,
                None,
                col.precision,
                col.scale,
                col.nullable,
            )
            for col in self._metadata.columns
        ]

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def setinputsizes(self, sizes: Sequence[Optional[Union[int, Type]]]) -> None:
        pass

    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        pass

    async def execute(
        self, operation: SQLQuery, parameters: Optional[QueryParameters] = None
    ) -> "AsyncCursor":
        logger.debug("Executing statement")
        opts = QueryOptions(parameters=parameters)
        self._query = self._job.query(operation, opts=opts)
        result = await self._query.run()
        self._buffer = list(result.data or [])
        self._is_done = result.is_done
        self._metadata = result.metadata
        self._rowcount = result.update_count if result.update_count is not None else -1
        return self

    async def executescript(self, script: SQLQuery) -> "AsyncCursor":
        """A lazy implementation of SQLite's `executescript`."""
        return await self.execute(script)

    async def executemany(
        self, operation: SQLQuery, seq_of_parameters: Sequence[QueryParameters]
    ) -> "AsyncCursor":
        for params in seq_of_parameters:
            await self.execute(operation, params)
        return self

    async def fetchone(self) -> Optional[ResultRow]:
        if self._buffer:
            return row_to_tuple(self._buffer.pop(0), self._metadata)
        if self._is_done or self._query is None:
            return None
        result = await self._query.fetch_more(rows_to_fetch=1)
        self._is_done = result.is_done
        self._buffer.extend(result.data or [])
        return row_to_tuple(self._buffer.pop(0), self._metadata) if self._buffer else None

    async def fetchmany(self, size: Optional[int] = None) -> ResultSet:
        if size is None:
            size = self.arraysize
        rows = []
        while len(rows) < size:
            if self._buffer:
                rows.append(row_to_tuple(self._buffer.pop(0), self._metadata))
            elif self._is_done or self._query is None:
                break
            else:
                result = await self._query.fetch_more(rows_to_fetch=size - len(rows))
                self._is_done = result.is_done
                self._buffer.extend(result.data or [])
        return rows

    async def fetchall(self) -> ResultSet:
        rows = [row_to_tuple(r, self._metadata) for r in self._buffer]
        self._buffer.clear()
        while not self._is_done and self._query is not None:
            result = await self._query.fetch_more(rows_to_fetch=100)
            self._is_done = result.is_done
            rows.extend(row_to_tuple(r, self._metadata) for r in (result.data or []))
        return rows

    async def close(self) -> None:
        logger.debug("Closing cursor")
        if self._query is not None:
            await self._query.close()
            self._query = None

    async def callproc(
        self, procname: ProcName, parameters: Optional[ProcArgs] = None
    ) -> Optional[ProcArgs]:
        await self.execute(procname, parameters)
        return parameters

    async def nextset(self) -> Optional[bool]:
        return None

    async def commit(self) -> None:
        await self._job.query("COMMIT").run()

    async def rollback(self) -> None:
        await self._job.query("ROLLBACK").run()
