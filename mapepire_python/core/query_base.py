"""
Base query architecture for unified query handling across sync and async operations.

This module provides the abstract base classes and interfaces for query execution,
using simple and clear patterns that avoid unnecessary complexity.
"""

import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

from ..data_types import QueryOptions

T = TypeVar("T")


class QueryState(Enum):
    """State of a query execution."""

    NOT_YET_RUN = 1
    RUN_MORE_DATA_AVAIL = 2
    RUN_DONE = 3
    ERROR = 4


class QueryResult(dict):
    """
    Unified result set interface for query results.
    
    Inherits from dict for perfect backward compatibility while providing
    modern property access and type safety.
    """

    def __init__(self, raw_result: Dict[str, Any]):
        # Initialize dict with raw result for full compatibility
        super().__init__(raw_result)
        
        # Cache properties for performance
        self._success = raw_result.get("success", False)
        self._is_done = raw_result.get("is_done", False)
        self._correlation_id = raw_result.get("id")
        self._error = raw_result.get("error")
        self._sql_state = raw_result.get("sql_state")
        self._sql_rc = raw_result.get("sql_rc")
        self._data = raw_result.get("data", [])
        self._metadata = raw_result.get("metadata", {})

    @property
    def success(self) -> bool:
        """Whether the query executed successfully."""
        return self._success

    @property
    def is_done(self) -> bool:
        """Whether all results have been fetched."""
        return self._is_done

    @property
    def correlation_id(self) -> Optional[str]:
        """Correlation ID for the query."""
        return self._correlation_id

    @property
    def error(self) -> Optional[str]:
        """Error message if query failed."""
        return self._error

    @property
    def sql_state(self) -> Optional[str]:
        """SQL state code if query failed."""
        return self._sql_state

    @property
    def sql_rc(self) -> Optional[int]:
        """SQL return code if query failed."""
        return self._sql_rc

    @property
    def data(self) -> List[Any]:
        """Query result data."""
        return self._data

    @property
    def metadata(self) -> Dict[str, Any]:
        """Query result metadata."""
        return self._metadata

    def get_error_details(self) -> Dict[str, Any]:
        """Get detailed error information."""
        error_details = {}
        if self._error:
            error_details["error"] = self._error
        if self._sql_state:
            error_details["sql_state"] = self._sql_state
        if self._sql_rc:
            error_details["sql_rc"] = self._sql_rc

        if not error_details and not self._success:
            error_details["error"] = "failed to run query for unknown reason"

        return error_details

    # Enhanced dict access with computed properties
    def __getitem__(self, key: str) -> Any:
        """Enhanced dict access that computes some properties dynamically."""
        if key == "has_results":
            return len(self._data) > 0
        elif key == "update_count":
            return self._metadata.get("update_count")
        else:
            # Use native dict access for all other keys
            return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        """Enhanced dict.get() with computed properties."""
        try:
            return self[key]
        except KeyError:
            return default


class BaseQueryExecutor(ABC):
    """Abstract base class for query execution strategies."""

    @abstractmethod
    def execute_query(self, job: Any, query_object: Dict[str, Any]) -> Union[Dict[str, Any], Any]:
        """Execute a query and return the result."""
        pass

    @abstractmethod
    def validate_connection(self, job: Any) -> bool:
        """Validate that the job connection is available."""
        pass


class SyncQueryExecutor(BaseQueryExecutor):
    """Synchronous query executor implementation."""

    def execute_query(self, job: Any, query_object: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a synchronous query."""
        job.send(json.dumps(query_object))
        query_result: Dict[str, Any] = json.loads(job._socket.recv())
        return query_result

    def validate_connection(self, job: Any) -> bool:
        """Validate synchronous connection."""
        return job._socket is not None


class AsyncQueryExecutor(BaseQueryExecutor):
    """Asynchronous query executor implementation."""

    async def execute_query(self, job: Any, query_object: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an asynchronous query."""
        query_result = await job.send(json.dumps(query_object))
        return query_result

    def validate_connection(self, job: Any) -> bool:
        """Validate asynchronous connection."""
        return job.socket is not None


class BaseQuery(ABC, Generic[T]):
    """
    Abstract base class for all query implementations.

    Provides common query lifecycle methods, parameter binding, and result processing
    while allowing for different execution strategies (sync/async).
    """

    def __init__(self, job: T, query: str, opts: QueryOptions, executor: BaseQueryExecutor) -> None:
        """Initialize base query with common properties."""
        self.job = job
        self.sql: str = query
        self.executor = executor

        # Query options
        self.is_prepared: bool = opts.parameters is not None
        self.parameters: Optional[List[str]] = opts.parameters or []
        self.is_cl_command: Optional[bool] = opts.isClCommand
        self.should_auto_close: Optional[bool] = opts.autoClose
        self.is_terse_results: Optional[bool] = opts.isTerseResults

        # Query state
        self._rows_to_fetch: int = 100
        self.state: QueryState = QueryState.NOT_YET_RUN
        self._correlation_id: Optional[str] = None

    def _validate_query_state(self, allowed_states: List[QueryState]) -> None:
        """Validate that the query is in an allowed state."""
        if self.state not in allowed_states:
            if self.state == QueryState.RUN_MORE_DATA_AVAIL:
                raise Exception("Statement has already been run")
            elif self.state == QueryState.RUN_DONE:
                raise Exception("Statement has already been fully run")
            elif self.state == QueryState.NOT_YET_RUN:
                raise Exception("Statement has not been run")
            elif self.state == QueryState.ERROR:
                raise Exception("Statement is in error state")

    def _build_query_object(
        self,
        operation_type: str,
        rows_to_fetch: Optional[int] = None,
        correlation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build query object for different operation types."""
        if operation_type == "run":
            if self.is_cl_command:
                return {
                    "id": self.job._get_unique_id("clcommand"),
                    "type": "cl",
                    "terse": self.is_terse_results,
                    "cmd": self.sql,
                }
            else:
                return {
                    "id": self.job._get_unique_id("query"),
                    "type": "prepare_sql_execute" if self.is_prepared else "sql",
                    "sql": self.sql,
                    "terse": self.is_terse_results,
                    "rows": rows_to_fetch or self._rows_to_fetch,
                    "parameters": self.parameters,
                }

        elif operation_type == "prepare_execute":
            return {
                "id": self.job._get_unique_id("prepare_sql_execute"),
                "type": "prepare_sql_execute",
                "sql": self.sql,
                "rows": 0,
                "parameters": self.parameters,
            }

        elif operation_type == "fetch_more":
            return {
                "id": self.job._get_unique_id("fetchMore"),
                "cont_id": correlation_id or self._correlation_id,
                "type": "sqlmore",
                "sql": self.sql,
                "rows": rows_to_fetch or self._rows_to_fetch,
            }

        elif operation_type == "close":
            return {
                "id": self.job._get_unique_id("sqlclose"),
                "cont_id": correlation_id or self._correlation_id,
                "type": "sqlclose",
            }

        else:
            raise ValueError(f"Unknown operation type: {operation_type}")

    def _process_query_result(self, raw_result: Dict[str, Any]) -> QueryResult:
        """Process raw query result into QueryResult object."""
        result = QueryResult(raw_result)

        # Update query state based on result
        if result.success or self.is_cl_command:
            self.state = QueryState.RUN_DONE if result.is_done else QueryState.RUN_MORE_DATA_AVAIL
            if result.correlation_id:
                self._correlation_id = result.correlation_id
        else:
            self.state = QueryState.ERROR

        return result

    def _handle_query_error(self, result: QueryResult) -> None:
        """Handle query execution errors."""
        if not result.success and not self.is_cl_command:
            error_details = result.get_error_details()
            raise RuntimeError(error_details)

    @abstractmethod
    def run(self, rows_to_fetch: Optional[int] = None) -> QueryResult:
        """Execute the query."""
        pass

    @abstractmethod
    def fetch_more(self, rows_to_fetch: Optional[int] = None) -> QueryResult:
        """Fetch more results from a previously executed query."""
        pass

    @abstractmethod
    def close(self) -> Optional[QueryResult]:
        """Close the query and clean up resources."""
        pass

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"job={str(self.job)}, "
            f"sql={self.sql}, "
            f"parameters={self.parameters}, "
            f"correlation_id={self._correlation_id})"
        )
