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

from ..client.query import Query
from ..client.sql_job import SQLJob
from ..core.exceptions import convert_runtime_errors
from ..core.metadata_processor import MetadataProcessor
from ..core.pep249_adapter import PEP249QueryAdapter
from ..core.result_processor import ResultSetProcessor
from ..core.utils import QueryResultSet

__all__ = ["Cursor"]


class Cursor(pep249.CursorConnectionMixin, pep249.IterableCursorMixin, pep249.TransactionalCursor):
    max_rows = 2147483647

    def __init__(self, connection: "Connection", job: SQLJob) -> None:
        super().__init__()
        self._connection = weakref.proxy(connection)
        self.job = job
        self.query_adapter = PEP249QueryAdapter(connection.query_factory)
        self.result_processor = ResultSetProcessor()
        self.metadata_processor = MetadataProcessor()
        self.query_q: deque[Query] = deque(maxlen=20)
        self.__closed = False
        self.__has_results = False
        self._result_set: Optional[QueryResultSet] = None

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
        """Execute query using unified architecture."""
        # Execute query through the adapter for consistent handling
        query_result = self.query_adapter.execute_query(operation, parameters, **kwargs)

        # Update cursor state based on results
        if query_result.has_results:
            self.__set_has_results(True)
        # Keep reference to query for legacy compatibility
        if self.query_adapter.current_query:
            self.query_q.append(self.query_adapter.current_query)

        # Set rowcount for DML operations
        if query_result.update_count:
            self.rowcount = query_result.update_count

        # Store result for metadata access
        self._result_set = QueryResultSet(query_result.get_formatted_result())

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
        """Column descriptions from the last executed query."""
        if not self.query_adapter.current_query or not self.query_adapter._last_result:
            return None
        return self.metadata_processor.extract_column_descriptions(self.query_adapter._last_result)

    @raise_if_closed
    @convert_runtime_errors
    def fetchone(self) -> Optional[ResultRow]:
        """Fetch next row from query results."""
        result = self.query_adapter.get_fetchone_result()
        if result:
            self._result_set = QueryResultSet(result)
        return result

    @raise_if_closed
    @convert_runtime_errors
    def fetchall(self) -> ResultSet:
        """Fetch all remaining rows from query results."""
        result = self.query_adapter.get_fetchall_result()
        if result:
            self._result_set = QueryResultSet(result)
        return result.get("data", [])

    @raise_if_closed
    @convert_runtime_errors
    def fetchmany(self, size: Optional[int] = None) -> ResultSet:
        """Fetch specified number of rows from query results."""
        if size is None:
            size = self.arraysize
        result = self.query_adapter.get_fetchmany_result(size)
        if result:
            self._result_set = QueryResultSet(result)
        return result.get("data", [])

    def executescript(self, script: SQLQuery) -> "Cursor":
        """A lazy implementation of SQLite's `executescript`."""
        return self.execute(script)

    def nextset(self) -> Optional[bool]:
        """Move to next result set if available."""
        try:
            if len(self.query_q) > 1:
                self.query_q.popleft()
                # Update adapter to use next query
                if self.query_q:
                    self.query_adapter.current_query = self.query_q[0]
                    self.__set_has_results(True)
                    return True
            return None
        except Exception:
            return None

    @convert_runtime_errors
    def close(self) -> None:
        """Close cursor and cleanup resources."""
        if self._closed:
            return

        # Close query adapter
        self.query_adapter.close_query()

        # Close any remaining queries in queue
        for q in self.query_q:
            try:
                q.close()
            except Exception:
                pass  # Ignore cleanup errors
        self.query_q.clear()

        self._result_set = None
        self._closed = True

    def commit(self) -> None:
        self.job.query_and_run("COMMIT")

    def rollback(self) -> None:
        self.job.query_and_run("ROLLBACK")
