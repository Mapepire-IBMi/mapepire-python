import json
from typing import Any, Dict, List, Optional

from mapepire_python.client.query import QueryState
from mapepire_python.data_types import QueryOptions
from mapepire_python.pool.pool_job import PoolJob


class PoolQuery:
    global_query_list: List["PoolQuery"] = []

    def __init__(self, job: PoolJob, query: str, opts: QueryOptions) -> None:
        self.job = job
        self.sql: str = query
        self.is_prepared: bool = True if opts.parameters is not None else False
        self.parameters: Optional[List[str]] = opts.parameters
        self.is_cl_command: Optional[bool] = opts.isClCommand
        self.should_auto_close: Optional[bool] = opts.autoClose
        self.is_terse_results: Optional[bool] = opts.isTerseResults

        self._rows_to_fetch: int = 100
        self.state: QueryState = QueryState.NOT_YET_RUN

        PoolQuery.global_query_list.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def _execute_query(self, qeury_object: Dict[str, Any]) -> Dict[Any, Any]:
        query_result = await self.job.send(json.dumps(qeury_object))
        return query_result

    async def run(self, rows_to_fetch: Optional[int] = None) -> Dict[str, Any]:
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

        query_result = await self._execute_query(query_object)

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

            raise Exception(error_list)

        self._correlation_id = query_result["id"]

        return query_result

    async def fetch_more(self, rows_to_fetch: Optional[int] = None) -> Dict[str, Any]:
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
        query_result: Dict[str, Any] = await self._execute_query(query_object)

        self.state = (
            QueryState.RUN_DONE
            if query_result.get("is_done", False)
            else QueryState.RUN_MORE_DATA_AVAIL
        )

        if not query_result["success"]:
            self.state = QueryState.ERROR
            raise Exception(query_result["error"] or "Failed to run Query (unknown error)")

        return query_result

    async def close(self):
        if not self.job.socket:
            raise Exception("SQL Job not connected")
        if self._correlation_id and self.state is not QueryState.RUN_DONE:
            self.state = QueryState.RUN_DONE
            query_object = {
                "id": self.job._get_unique_id("sqlclose"),
                "cont_id": self._correlation_id,
                "type": "sqlclose",
            }

            return await self._execute_query(query_object)
        elif not self._correlation_id:
            self.state = QueryState.RUN_DONE
