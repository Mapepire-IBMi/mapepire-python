import weakref
from collections import deque
from typing import TYPE_CHECKING, Any, Optional, Sequence, Type, Union

import pep249
from pep249 import (
    ColumnDescription,
    ProcArgs,
    ProcName,
    QueryParameters,
    ResultRow,
    ResultSet,
    SQLQuery,
)
from pep249.cursor import CursorType

from mapepire_python.core.utils import raise_if_closed

if TYPE_CHECKING:
    # pylint: disable=cyclic-import
    from ..core.connection import Connection

from ..client.query import Query, QueryState
from ..client.sql_job import SQLJob
from ..core.exceptions import convert_runtime_errors
from ..core.utils import QueryResultSet
from ..data_types import QueryOptions

__all__ = ["Cursor"]


class Cursor(pep249.CursorConnectionMixin, pep249.IterableCursorMixin, pep249.TransactionalCursor):
    max_rows = 2147483647

    def __init__(self, connection: "Connection", job: SQLJob) -> None:
        super().__init__()
        self._connection = weakref.proxy(connection)
        self.job = job
        self.query: Query = None
        self.query_q: deque[Query] = deque(maxlen=20)
        self.__closed = False
        self.__has_results = False

    @property
    def has_results(self) -> bool:
        return self.__has_results

    def __set_has_results(self, value: bool):
        self.__has_results = value

    @property
    def connection(self) -> "Connection":
        """The parent Connection of the implementing cursor."""
        return self._connection

    @property
    def _closed(self) -> bool:
        # pylint: disable=protected-access
        try:
            return self.__closed or self.connection._closed
        except ReferenceError:
            # Parent connection already GC'd.
            return True

    @_closed.setter
    def _closed(self, value: bool):
        self.__closed = value

    @property
    def rowcount(self) -> int:
        return getattr(self, "_rowcount", -1)

    @rowcount.setter
    def rowcount(self, value: int):
        setattr(self, "_rowcount", value)

    def setinputsizes(self, sizes: Sequence[Optional[Union[int, Type]]]) -> None:
        pass

    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        pass

    @raise_if_closed
    @convert_runtime_errors
    def execute(
        self,
        operation: SQLQuery,
        parameters: Optional[QueryParameters] = None,
        **kwargs: Any,
    ) -> "Cursor":
        opts = kwargs.get("opts", None)
        if opts:
            query = Query(self.job, operation, opts)
        else:
            create_opts = QueryOptions(
                isClCommand=kwargs.get("isClCommand", None),
                isTerseResults=kwargs.get("isTerseResults", None),
                parameters=parameters if parameters else None,
                autoClose=kwargs.get("autoclose", None),
            )

            query = Query(self.job, operation, create_opts)

        prepare_result = query.prepare_sql_execute()

        if prepare_result["has_results"]:
            self.query = query
            self.__set_has_results(True)
            self.query_q.append(query)

        update_count = prepare_result.get("update_count", None)
        if update_count:
            self.rowcount = update_count

        return self

    @raise_if_closed
    @convert_runtime_errors
    def executemany(
        self: CursorType,
        operation: SQLQuery,
        seq_of_parameters: Sequence[QueryParameters],
        **kwargs: Any,
    ) -> "Cursor":
        return self.execute(operation=operation, parameters=seq_of_parameters)

    @raise_if_closed
    @convert_runtime_errors
    def callproc(self, procname: ProcName, parameters: Optional[ProcArgs] = None) -> "Cursor":
        return self.execute(procname, parameters=parameters)

    @property
    def description(
        self,
    ) -> Optional[Sequence[ColumnDescription]]:
        pass

    @raise_if_closed
    @convert_runtime_errors
    def fetchone(self) -> Optional[ResultRow]:
        if not self.query or self.query.state == QueryState.RUN_DONE:
            return None
        res = self.query.fetch_more(rows_to_fetch=1)
        if res:
            self._result_set = QueryResultSet(res)
        return res

    @raise_if_closed
    @convert_runtime_errors
    def fetchall(self) -> ResultSet:
        if not self.query:
            return None

        res = self.query.fetch_more(rows_to_fetch=self.max_rows)
        if res:
            self._result_set = QueryResultSet(res)
            return res

    @raise_if_closed
    @convert_runtime_errors
    def fetchmany(self, size: Optional[int] = None) -> ResultSet:
        if size is None:
            size = self.arraysize
        if not self.query:
            return None
        res = self.query.fetch_more(rows_to_fetch=size)
        if res:
            self._result_set = QueryResultSet(res)
        return res

    def executescript(self, script: SQLQuery) -> "Cursor":
        """A lazy implementation of SQLite's `executescript`."""
        return self.execute(script)

    def nextset(self) -> Optional[bool]:
        try:
            if len(self.query_q) > 1:
                self.query_q.popleft()
                self.query = self.query_q[0]
                self.__set_has_results(True)
                return True
            return None
        except Exception:
            return None

    @convert_runtime_errors
    def close(self) -> None:
        if self._closed:
            return
        if self.query:
            for q in self.query_q:
                q.close()
            self.query_q.clear()
            self._closed = True

    def commit(self) -> None:
        self.job.query_and_run("COMMIT")

    def rollback(self) -> None:
        self.job.query_and_run("ROLLBACK")
