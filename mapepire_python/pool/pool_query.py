import dataclasses
import json
from typing import Any, Dict, List, Optional

from mapepire_python.client.query import QueryState
from mapepire_python.data_types import (
    BaseRequest,
    ClRequest,
    PrepareSqlExecuteRequest,
    QueryOptions,
    QueryResult,
    SqlCloseRequest,
    SqlCloseResponse,
    SqlMoreRequest,
    SqlMoreResponse,
    SqlRequest,
)
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
        self._correlation_id: Optional[str] = None

        PoolQuery.global_query_list.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def _execute_query(self, request: BaseRequest) -> Dict[Any, Any]:
        query_result = await self.job.send(json.dumps(dataclasses.asdict(request)))
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

        if self.is_cl_command:
            request = ClRequest(
                id=self.job._get_unique_id("clcommand"),
                cmd=self.sql,
                terse=self.is_terse_results,
            )
        elif self.is_prepared:
            request = PrepareSqlExecuteRequest(
                id=self.job._get_unique_id("query"),
                sql=self.sql,
                terse=self.is_terse_results,
                rows=rows_to_fetch,
                parameters=self.parameters,
            )
        else:
            request = SqlRequest(
                id=self.job._get_unique_id("query"),
                sql=self.sql,
                terse=self.is_terse_results,
                rows=rows_to_fetch,
                parameters=self.parameters,
            )

        query_result = QueryResult.from_dict(await self._execute_query(request))  # type: ignore

        self.state = QueryState.RUN_DONE if query_result.is_done else QueryState.RUN_MORE_DATA_AVAIL

        if not query_result.success and not self.is_cl_command:
            self.state = QueryState.ERROR
            error_list = {k: v for k, v in {"error": query_result.error, "sql_state": query_result.sql_state, "sql_rc": query_result.sql_rc}.items() if v is not None}
            if not error_list:
                error_list["error"] = "failed to run query for unknown reason"
            raise Exception(error_list)

        self._correlation_id = query_result.id

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

        assert self._correlation_id is not None
        self._rows_to_fetch = rows_to_fetch
        query_result = SqlMoreResponse.from_dict(  # type: ignore
            await self._execute_query(
                SqlMoreRequest(
                    id=self.job._get_unique_id("fetchMore"),
                    cont_id=self._correlation_id,
                    sql=self.sql,
                    rows=rows_to_fetch,
                )
            )
        )

        self.state = QueryState.RUN_DONE if query_result.is_done else QueryState.RUN_MORE_DATA_AVAIL

        if not query_result.success:
            self.state = QueryState.ERROR
            raise Exception(query_result.error or "Failed to run Query (unknown error)")

        return query_result

    async def close(self):
        if not self.job.socket:
            raise Exception("SQL Job not connected")
        if self._correlation_id and self.state is not QueryState.RUN_DONE:
            self.state = QueryState.RUN_DONE
            return SqlCloseResponse.from_dict(  # type: ignore
                await self._execute_query(
                    SqlCloseRequest(
                        id=self.job._get_unique_id("sqlclose"),
                        cont_id=self._correlation_id,
                    )
                )
            )
        elif not self._correlation_id:
            self.state = QueryState.RUN_DONE
