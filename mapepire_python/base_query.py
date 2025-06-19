from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

from mapepire_python.data_types import QueryOptions

T = TypeVar("T")


class QueryState(Enum):
    NOT_YET_RUN = (1,)
    RUN_MORE_DATA_AVAIL = (2,)
    RUN_DONE = (3,)
    ERROR = 4


class BaseQuery(ABC, Generic[T]):
    def __init__(self, job: T, query: str, opts: QueryOptions) -> None:
        self.job = job
        self.sql: str = query
        self.is_prepared: bool = True if opts.parameters is not None else False
        self.parameters: Optional[List[str]] = opts.parameters or []
        self.is_cl_command: Optional[bool] = opts.isClCommand
        self.should_auto_close: Optional[bool] = opts.autoClose
        self.is_terse_results: Optional[bool] = opts.isTerseResults

        self._rows_to_fetch: int = 100
        self.state: QueryState = QueryState.NOT_YET_RUN
        self._correlation_id: Optional[str] = None

    def __str__(self):
        return f"{self.__class__.__name__}(job={str(self.job)}, sql={self.sql}, parameters={self.parameters}, correlation_id={self._correlation_id})"

    @abstractmethod
    def _execute_query(self, query_object: Dict[str, Any]) -> Dict[str, Any]:
        """Execute query via the job's communication method (sync/async)"""
        pass

    @abstractmethod
    def _get_job_socket_attr(self) -> str:
        """Return the attribute name for the job's socket connection"""
        pass

    @abstractmethod
    def _create_runtime_error(self, error_data: Union[Dict[str, Any], str]) -> Exception:
        """Create appropriate exception type for sync/async contexts"""
        pass

    def _validate_query_state_for_run(self) -> None:
        """Validate query state before running"""
        if self.state == QueryState.RUN_MORE_DATA_AVAIL:
            raise Exception("Statement has already been run")
        elif self.state == QueryState.RUN_DONE:
            raise Exception("Statement has already been fully run")

    def _validate_query_state_for_fetch_more(self) -> None:
        """Validate query state before fetching more"""
        if self.state == QueryState.NOT_YET_RUN:
            raise Exception("Statement has not been run")
        elif self.state == QueryState.RUN_DONE:
            raise Exception("Statement has already been fully run")

    def _build_query_object(self, rows_to_fetch: int, operation_type: str) -> Dict[str, Any]:
        """Build query object for different operations"""
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
                    "rows": rows_to_fetch,
                    "parameters": self.parameters,
                }
        elif operation_type == "fetch_more":
            return {
                "id": self.job._get_unique_id("fetchMore"),
                "cont_id": self._correlation_id,
                "type": "sqlmore",
                "sql": self.sql,
                "rows": rows_to_fetch,
            }
        elif operation_type == "close":
            return {
                "id": self.job._get_unique_id("sqlclose"),
                "cont_id": self._correlation_id,
                "type": "sqlclose",
            }
        else:
            raise ValueError(f"Unknown operation type: {operation_type}")

    def _process_query_result(self, query_result: Dict[str, Any]) -> None:
        """Process query result and update state"""
        self.state = (
            QueryState.RUN_DONE
            if query_result.get("is_done", False)
            else QueryState.RUN_MORE_DATA_AVAIL
        )

        if not query_result.get("success", False) and not self.is_cl_command:
            self.state = QueryState.ERROR
            error_keys = ["error", "sql_state", "sql_rc"]
            error_list = {
                key: query_result[key] for key in error_keys if key in query_result.keys()
            }
            if len(error_list) == 0:
                error_list["error"] = "failed to run query for unknown reason"

            raise self._create_runtime_error(error_list)

        self._correlation_id = query_result["id"]

    def _process_fetch_more_result(self, query_result: Dict[str, Any]) -> None:
        """Process fetch more result and update state"""
        self.state = (
            QueryState.RUN_DONE
            if query_result.get("is_done", False)
            else QueryState.RUN_MORE_DATA_AVAIL
        )

        if not query_result["success"]:
            self.state = QueryState.ERROR
            error_msg = query_result["error"] or "Failed to run Query (unknown error)"
            raise self._create_runtime_error(error_msg)

    def _should_close_query(self) -> bool:
        """Check if query should be closed"""
        job_socket = getattr(self.job, self._get_job_socket_attr())
        return (
            job_socket is not None
            and self._correlation_id is not None
            and self.state is not QueryState.RUN_DONE
        )

    def _handle_close_without_correlation_id(self) -> None:
        """Handle close when no correlation ID exists"""
        if not self._correlation_id:
            self.state = QueryState.RUN_DONE