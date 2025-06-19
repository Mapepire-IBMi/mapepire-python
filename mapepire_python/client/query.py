import json
from typing import Any, Dict, List, Optional

from mapepire_python.websocket import handle_ws_errors
from mapepire_python.base_query import BaseQuery, QueryState

from ..data_types import QueryOptions
from .sql_job import SQLJob


class Query(BaseQuery[SQLJob]):
    global_query_list: List["Query"] = []

    def __init__(self, job: SQLJob, query: str, opts: QueryOptions) -> None:
        super().__init__(job, query, opts)
        Query.global_query_list.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _execute_query(self, query_object: Dict[str, Any]) -> Dict[str, Any]:
        self.job.send(json.dumps(query_object))
        query_result: Dict[str, Any] = json.loads(self.job._socket.recv())
        return query_result

    def _get_job_socket_attr(self) -> str:
        return "_socket"

    def _create_runtime_error(self, error_data) -> Exception:
        return RuntimeError(error_data)

    @handle_ws_errors
    def prepare_sql_execute(self):
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
        self._process_query_result(query_result)
        return query_result

    @handle_ws_errors
    def run(self, rows_to_fetch: Optional[int] = None) -> Dict[str, Any]:
        if rows_to_fetch is None:
            rows_to_fetch = self._rows_to_fetch
        else:
            self._rows_to_fetch = rows_to_fetch

        self._validate_query_state_for_run()
        query_object = self._build_query_object(rows_to_fetch, "run")
        query_result: Dict[str, Any] = self._execute_query(query_object)
        self._process_query_result(query_result)
        return query_result

    @handle_ws_errors
    def fetch_more(self, rows_to_fetch: Optional[int] = None) -> Dict[str, Any]:
        if rows_to_fetch is None:
            rows_to_fetch = self._rows_to_fetch
        else:
            self._rows_to_fetch = rows_to_fetch

        self._validate_query_state_for_fetch_more()
        query_object = self._build_query_object(rows_to_fetch, "fetch_more")
        query_result: Dict[str, Any] = self._execute_query(query_object)
        self._process_fetch_more_result(query_result)
        return query_result

    @handle_ws_errors
    def close(self):
        job_socket = getattr(self.job, self._get_job_socket_attr())
        if not job_socket:
            raise Exception("SQL Job not connected")
        
        if self._should_close_query():
            self.state = QueryState.RUN_DONE
            query_object = self._build_query_object(0, "close")
            return self._execute_query(query_object)
        else:
            self._handle_close_without_correlation_id()
