import json
from typing import Any, Dict, List, Optional

from mapepire_python.base_query import BaseQuery, QueryState
from mapepire_python.data_types import QueryOptions
from mapepire_python.pool.pool_job import PoolJob


class PoolQuery(BaseQuery[PoolJob]):
    global_query_list: List["PoolQuery"] = []

    def __init__(self, job: PoolJob, query: str, opts: QueryOptions) -> None:
        super().__init__(job, query, opts)
        PoolQuery.global_query_list.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def _execute_query(self, query_object: Dict[str, Any]) -> Dict[str, Any]:
        query_result = await self.job.send(json.dumps(query_object))
        return query_result

    def _get_job_socket_attr(self) -> str:
        return "socket"

    def _create_runtime_error(self, error_data) -> Exception:
        return Exception(error_data)

    async def run(self, rows_to_fetch: Optional[int] = None) -> Dict[str, Any]:
        if rows_to_fetch is None:
            rows_to_fetch = self._rows_to_fetch
        else:
            self._rows_to_fetch = rows_to_fetch

        self._validate_query_state_for_run()
        query_object = self._build_query_object(rows_to_fetch, "run")
        query_result = await self._execute_query(query_object)
        self._process_query_result(query_result)
        return query_result

    async def fetch_more(self, rows_to_fetch: Optional[int] = None) -> Dict[str, Any]:
        if rows_to_fetch is None:
            rows_to_fetch = self._rows_to_fetch
        else:
            self._rows_to_fetch = rows_to_fetch

        self._validate_query_state_for_fetch_more()
        query_object = self._build_query_object(rows_to_fetch, "fetch_more")
        query_result: Dict[str, Any] = await self._execute_query(query_object)
        self._process_fetch_more_result(query_result)
        return query_result

    async def close(self):
        job_socket = getattr(self.job, self._get_job_socket_attr())
        if not job_socket:
            raise Exception("SQL Job not connected")
        
        if self._should_close_query():
            self.state = QueryState.RUN_DONE
            query_object = self._build_query_object(0, "close")
            return await self._execute_query(query_object)
        else:
            self._handle_close_without_correlation_id()
