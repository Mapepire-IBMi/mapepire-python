from typing import Any, Dict, Sequence, Union

from pep249 import aiopep249
from pep249.aiopep249.types import ProcArgs, ProcName, SQLQuery

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.core.connection import Connection
from mapepire_python.data_types import DaemonServer

from .cursor import AsyncCursor
from .utils import to_thread


class AsyncConnection(aiopep249.AsyncCursorExecuteMixin, aiopep249.AsyncConnection):

    def __init__(self, database: Union[DaemonServer, dict], opts={}) -> None:
        super().__init__()
        self._connection = Connection(database, opts=opts)

    async def cursor(self) -> AsyncCursor:
        return AsyncCursor(self, self._connection.cursor())

    async def close(self) -> None:
        await to_thread(self._connection.close)

    async def execute(
        self, operation: str, parameters: Sequence[Any] | Dict[str | int, Any] | None = None
    ) -> AsyncCursor:
        cursor = await self.cursor()
        return await cursor.execute(operation, parameters)

    async def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any] | Dict[str | int, Any]]
    ) -> AsyncCursor:
        cursor = await self.cursor()
        return await cursor.executemany(operation, seq_of_parameters)

    async def callproc(
        self, procname: str, parameters: Sequence[Any] | None = None
    ) -> Sequence[Any] | None:
        raise NotImplementedError

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass
