import json
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

from python_wsdb.client.sql_job import SQLJob
from python_wsdb.types import QueryOptions

T = TypeVar("T")


class QueryState(Enum):
    NOT_YET_RUN = (1,)
    RUN_MORE_DATA_AVAIL = (2,)
    RUN_DONE = (3,)
    ERROR = 4


def get_query_options(opts: Optional[Union[Dict[str, Any], QueryOptions]] = None) -> QueryOptions:
    if isinstance(opts, QueryOptions):
        return opts
    elif opts:
        return QueryOptions(**opts)
    else:
        return QueryOptions(isClCommand=False, parameters=None, autoClose=False)


class Query(Generic[T]):
    global_query_list: List["Query[Any]"] = []

    def __init__(self, job: SQLJob, query: str, opts: QueryOptions) -> None:
        self.job = job
        self.is_prepared: bool = True if opts.parameters is not None else False
        self.parameters: Optional[List[str]] = opts.parameters
        self.sql: str = query
        self.is_cl_command: Optional[bool] = opts.isClCommand
        self.should_auto_close: Optional[bool] = opts.autoClose
        self.is_terse_results: Optional[bool] = opts.isTerseResults

        self._rows_to_fetch: int = 100
        self.state: QueryState = QueryState.NOT_YET_RUN

        Query.global_query_list.append(self)

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

        self.job.send(json.dumps(query_object))
        query_result: Dict[str, Any] = json.loads(self.job._socket.recv())

        self.state = (
            QueryState.RUN_DONE
            if query_result.get("is_done", False)
            else QueryState.RUN_MORE_DATA_AVAIL
        )

        if not query_result.get("success", False) and not self.is_cl_command:
            print(query_result)
            self.state = QueryState.ERROR
            error_keys = ["error", "sql_state", "sql_rc"]
            error_list = {
                key: query_result[key] for key in error_keys if key in query_result.keys()
            }
            if len(error_list) == 0:
                error_list["error"] = "failed to run query for unknown reason"

            raise Exception(error_list)

        self._correlation_id = query_result["id"]

        return query_result

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
        self.job.send(json.dumps(query_object))
        query_result: Dict[str, Any] = json.loads(self.job._socket.recv())

        self.state = (
            QueryState.RUN_DONE
            if query_result.get("is_done", False)
            else QueryState.RUN_MORE_DATA_AVAIL
        )

        if not query_result["success"]:
            self.state = QueryState.ERROR
            raise Exception(query_result["error"] or "Failed to run Query (unknown error)")

        return query_result
