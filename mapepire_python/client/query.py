import json
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar

from mapepire_python.websocket import handle_ws_errors

from ..data_types import QueryOptions
from .sql_job import SQLJob

T = TypeVar("T")


class QueryState(Enum):
    NOT_YET_RUN = (1,)
    RUN_MORE_DATA_AVAIL = (2,)
    RUN_DONE = (3,)
    ERROR = 4


class Query(Generic[T]):
    global_query_list: List["Query[Any]"] = []

    def __init__(self, job: SQLJob, query: str, opts: QueryOptions) -> None:
        self.job = job
        self.sql: str = query
        self.is_prepared: bool = True if opts.parameters is not None else False
        self.parameters: Optional[List[str]] = opts.parameters or []
        self.is_cl_command: Optional[bool] = opts.isClCommand
        self.should_auto_close: Optional[bool] = opts.autoClose
        self.is_terse_results: Optional[bool] = opts.isTerseResults

        self._rows_to_fetch: int = 100
        self.state: QueryState = QueryState.NOT_YET_RUN
        self._correlation_id = None
        Query.global_query_list.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __str__(self):
        return f"Query(job={str(self.job)}, sql={self.sql}, parameters={self.parameters}, correlation_id={self._correlation_id})"

    def _execute_query(self, qeury_object: Dict[str, Any]) -> Dict[str, Any]:
        self.job.send(json.dumps(qeury_object))
        query_result: Dict[str, Any] = json.loads(self.job._socket.recv())
        return query_result

    @handle_ws_errors
    def prepare_sql_execute(self):
        # check Query state first
        if self.state == QueryState.RUN_DONE:
            raise Exception("Statement has already been fully run")

        query_object = {
            "id": self.job._get_unique_id("prepare_sql_execute"),
            "type": "prepare_sql_execute",
            "sql": self.sql,
            "rows": 0,
            "parameters": self.parameters,
        }

        query_result: Dict[str, Any] = self._execute_query(query_object)
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

            raise RuntimeError(error_list)

        self._correlation_id = query_result["id"]

        return query_result

    @handle_ws_errors
    def run(self, rows_to_fetch: Optional[int] = None) -> Dict[str, Any]:
        if rows_to_fetch is None:
            rows_to_fetch = self._rows_to_fetch
        else:
            self._rows_to_fetch = rows_to_fetch

        # check Query state first
        if self.state == QueryState.RUN_MORE_DATA_AVAIL:
            raise Exception("Statement has already been run")
        elif self.state == QueryState.RUN_DONE:
            raise Exception("Statement has already been fully run")

        query_object: Dict[str, Any] = {}
        if self.is_cl_command:
            query_object = {
                "id": self.job._get_unique_id("clcommand"),
                "type": "cl",
                "terse": self.is_terse_results,
                "cmd": self.sql,
            }
        else:
            query_object = {
                "id": self.job._get_unique_id("query"),
                "type": "prepare_sql_execute" if self.is_prepared else "sql",
                "sql": self.sql,
                "terse": self.is_terse_results,
                "rows": rows_to_fetch,
                "parameters": self.parameters,
            }

        query_result: Dict[str, Any] = self._execute_query(query_object)

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

            raise RuntimeError(error_list)

        self._correlation_id = query_result["id"]

        return query_result

    @handle_ws_errors
    def fetch_more(self, rows_to_fetch: Optional[int] = None) -> Dict[str, Any]:
        if rows_to_fetch is None:
            rows_to_fetch = self._rows_to_fetch
        else:
            self._rows_to_fetch = rows_to_fetch

        if self.state == QueryState.NOT_YET_RUN:
            raise Exception("Statement has not been run")
        elif self.state == QueryState.RUN_DONE:
            raise Exception("Statement has already been fully run")

        query_object = {
            "id": self.job._get_unique_id("fetchMore"),
            "cont_id": self._correlation_id,
            "type": "sqlmore",
            "sql": self.sql,
            "rows": rows_to_fetch,
        }

        self._rows_to_fetch = rows_to_fetch
        query_result: Dict[str, Any] = self._execute_query(query_object)

        self.state = (
            QueryState.RUN_DONE
            if query_result.get("is_done", False)
            else QueryState.RUN_MORE_DATA_AVAIL
        )

        if not query_result["success"]:
            self.state = QueryState.ERROR
            raise RuntimeError(query_result["error"] or "Failed to run Query (unknown error)")

        return query_result

    @handle_ws_errors
    def close(self):
        if not self.job._socket:
            raise Exception("SQL Job not connected")
        if self._correlation_id and self.state is not QueryState.RUN_DONE:
            self.state = QueryState.RUN_DONE
            query_object = {
                "id": self.job._get_unique_id("sqlclose"),
                "cont_id": self._correlation_id,
                "type": "sqlclose",
            }

            return self._execute_query(query_object)
        elif not self._correlation_id:
            self.state = QueryState.RUN_DONE
