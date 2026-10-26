import logging
import weakref
from collections import deque
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Deque,
    List,
    Optional,
    Sequence,
    Type,
    Union,
    cast,
)

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

from ..client.query import Query, QueryState
from ..client.sql_job import SQLJob
from ..core.exceptions import convert_runtime_errors
from ..core.utils import DB_TYPE_MAP, QueryResultSet, raise_if_closed, row_to_tuple
from ..data_types import QueryOptions

if TYPE_CHECKING:
    # pylint: disable=cyclic-import
    from ..core.connection import Connection

__all__ = ["Cursor"]

logger = logging.getLogger(__name__)


@dataclass
class _ResultState:
    """Fetch state for one query: its column metadata and rows received but not yet returned."""

    metadata: Any
    rows: Deque[Any] = field(default_factory=deque)


class Cursor(pep249.CursorConnectionMixin, pep249.IterableCursorMixin, pep249.TransactionalCursor):
    max_rows = 2147483647
    # Rows requested per round trip when fetchone() (and so iteration) needs more data.
    fetch_size = 100

    def __init__(self, connection: "Connection", job: SQLJob) -> None:
        super().__init__()
        self._connection = weakref.proxy(connection)
        self.job = job
        self.query: Optional[Query] = None
        self.query_q: deque[Query] = deque(maxlen=20)
        # Keyed weakly so a query evicted from query_q takes its buffered rows with it.
        self._results: "weakref.WeakKeyDictionary[Query, _ResultState]" = (
            weakref.WeakKeyDictionary()
        )
        self._metadata = None
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
        logger.debug("Executing statement")
        opts = kwargs.get("opts", None)
        if opts:
            query = Query(self.job, operation, opts)
        else:
            create_opts = QueryOptions(
                isClCommand=kwargs.get("isClCommand", None),
                isRemoteClCommand=kwargs.get("isRemoteClCommand", None),
                isTerseResults=kwargs.get("isTerseResults", None),
                parameters=parameters if parameters else None,
                autoClose=kwargs.get("autoclose", None),
            )

            query = Query(self.job, operation, create_opts)

        if query.is_cl_command:
            # CL commands return messages, not rows, and Query.run() does not raise when they fail.
            cl_result = query.run()
            self._metadata = None
            self.__set_has_results(False)
            self.rowcount = -1
            if not cl_result["success"]:
                raise RuntimeError(
                    {"error": cl_result["error"] or "CL command failed for unknown reason"}
                )
            return self

        prepare_result = query.prepare_sql_execute()

        qs = QueryResultSet(prepare_result)
        self._metadata = qs.metadata if qs.metadata.columns else None

        if prepare_result.has_results:
            self.query = query
            self.__set_has_results(True)
            self.query_q.append(query)
            # Rows that arrive with the execute response are served by the first fetch.
            self._results[query] = _ResultState(self._metadata, deque(qs.data or []))
        else:
            self.__set_has_results(False)

        if prepare_result.update_count is not None:
            self.rowcount = prepare_result.update_count

        return self

    @raise_if_closed
    @convert_runtime_errors
    def executemany(
        self,
        operation: SQLQuery,
        seq_of_parameters: Sequence[QueryParameters],
        **kwargs: Any,
    ) -> "Cursor":
        return self.execute(operation=operation, parameters=cast(QueryParameters, seq_of_parameters))

    @raise_if_closed
    @convert_runtime_errors
    def callproc(self, procname: ProcName, parameters: Optional[ProcArgs] = None) -> Optional[ProcArgs]:
        self.execute(procname, parameters=parameters)
        return parameters

    @property
    def description(
        self,
    ) -> Optional[Sequence[ColumnDescription]]:
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

    def _fetch_rows(self, size: int, batch_size: int = 0) -> ResultSet:
        """Return up to ``size`` rows of the current query as tuples.

        Buffered rows are served first; the server is only asked for more once
        the buffer is empty, and then for at least ``batch_size`` rows. Once the
        result set is exhausted this returns what it has (possibly ``[]``)
        instead of raising.
        """
        if self.query is None:
            return []
        state = self._results[self.query]
        rows: List[ResultRow] = []
        while len(rows) < size:
            if not state.rows:
                if self.query.state == QueryState.RUN_DONE:
                    break
                res = self.query.fetch_more(rows_to_fetch=max(size - len(rows), batch_size))
                if not res.data:
                    break
                state.rows.extend(res.data)
            rows.append(row_to_tuple(state.rows.popleft(), state.metadata))
        return rows

    @raise_if_closed
    @convert_runtime_errors
    def fetchone(self) -> Optional[ResultRow]:
        rows = self._fetch_rows(1, batch_size=self.fetch_size)
        return rows[0] if rows else None

    @raise_if_closed
    @convert_runtime_errors
    def fetchall(self) -> ResultSet:
        return self._fetch_rows(self.max_rows)

    @raise_if_closed
    @convert_runtime_errors
    def fetchmany(self, size: Optional[int] = None) -> ResultSet:
        if size is None:
            size = self.arraysize
        return self._fetch_rows(size)

    def executescript(self, script: SQLQuery) -> "Cursor":
        """A lazy implementation of SQLite's `executescript`."""
        return self.execute(script)

    def nextset(self) -> Optional[bool]:
        try:
            if len(self.query_q) > 1:
                self.query_q.popleft()
                self.query = self.query_q[0]
                self._metadata = self._results[self.query].metadata
                self.__set_has_results(True)
                return True
            return None
        except Exception as e:
            logger.exception("Error in nextset: %s", e)
            return None

    @convert_runtime_errors
    def close(self) -> None:
        if self._closed:
            return
        logger.debug("Closing cursor")
        for q in self.query_q:
            q.close()
        self.query_q.clear()
        self._results.clear()
        self._closed = True

    def commit(self) -> None:
        self.job.query_and_run("COMMIT")

    def rollback(self) -> None:
        self.job.query_and_run("ROLLBACK")
