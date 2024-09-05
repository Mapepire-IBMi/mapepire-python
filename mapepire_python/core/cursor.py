from collections import deque
from typing import Any, Dict, Optional, Sequence, Tuple, Type

import pep249
from pep249.cursor import CursorType

from mapepire_python.core.utils import raise_if_closed

from ..client.query import Query, QueryState
from ..client.sql_job import SQLJob
from ..core import QueryResultSet
from ..core.exceptions import convert_runtime_errors
from ..data_types import QueryOptions


class Cursor(pep249.CursorConnectionMixin, pep249.IterableCursorMixin, pep249.TransactionalCursor):
    max_rows = 2147483647

    def __init__(self, connection: "Connection", job: SQLJob) -> None:
        super().__init__()
        self.job = job
        self._connection = connection
        self.query: Query = None
        self.query_q = deque(maxlen=20)
        self.__closed = False

    @property
    def connection(self) -> "BaseConnection":
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

    def setinputsizes(self, sizes: Sequence[int | Type | None]) -> None:
        pass

    def setoutputsize(self, size: int, column: int | None) -> None:
        pass

    @raise_if_closed
    @convert_runtime_errors
    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | Dict[str | int, Any] | None = None,
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
        # print(prepare_result)

        if prepare_result["has_results"]:
            self.query = query
            self.query_q.append(query)

        update_count = prepare_result.get("update_count", None)
        if update_count:
            self.rowcount = update_count

        return self

    @raise_if_closed
    @convert_runtime_errors
    def executemany(
        self: CursorType,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any] | Dict[str | int, Any]],
        **kwargs: Any,
    ) -> "Cursor":
        return self.execute(operation=operation, parameters=seq_of_parameters)

    @raise_if_closed
    @convert_runtime_errors
    def callproc(
        self, procname: str, parameters: Sequence[Any] | None = None
    ) -> Sequence[Any] | None:
        return self.execute(procname, parameters=parameters)

    @property
    def description(
        self,
    ) -> (
        Sequence[Tuple[str, type, int | None, int | None, int | None, int | None, bool | None]]
        | None
    ):
        pass

    @raise_if_closed
    @convert_runtime_errors
    def fetchone(self) -> Sequence[Any] | Dict[str, Any] | None:
        if not self.query or self.query.state == QueryState.RUN_DONE:
            return None
        res = self.query.fetch_more(rows_to_fetch=1)
        if res:
            self._result_set = QueryResultSet(res)
        return res

    @raise_if_closed
    @convert_runtime_errors
    def fetchall(self) -> Sequence[Sequence[Any] | Dict[str, Any]]:
        if not self.query:
            return None
        print(self.query.sql)
        res = self.query.fetch_more(rows_to_fetch=self.max_rows)
        if res:
            self._result_set = QueryResultSet(res)
            return res

    @raise_if_closed
    @convert_runtime_errors
    def fetchmany(self, size: Optional[int] = None):
        if size is None:
            size = self.arraysize
        if not self.query:
            return None
        res = self.query.fetch_more(rows_to_fetch=size)
        if res:
            self._result_set = QueryResultSet(res)
        return res

    def nextset(self) -> bool | None:
        try:
            if len(self.query_q) > 1:
                self.query_q.popleft()
                self.query = self.query_q[0]
                return True
            return None
        except Exception:
            return None

    @convert_runtime_errors
    def close(self) -> None:
        if self._closed:
            return
        if self.query and self.job._socket.connected:
            self.query.close()
            self._closed = True

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass
