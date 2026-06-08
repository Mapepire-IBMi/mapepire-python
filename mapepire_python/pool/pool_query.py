import dataclasses
import json
import logging
from typing import Any, Dict, Mapping, Optional, Protocol, Sequence, Union

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

logger = logging.getLogger(__name__)


class _SQLJobProtocol(Protocol):
    """Structural protocol describing the job interface PoolQuery requires.

    Any job class that provides these three members — socket, send(), and
    _get_unique_id() — works with PoolQuery without needing to subclass it.
    Both PoolJob and AsyncSQLJob satisfy this protocol.
    """

    socket: Any

    def _get_unique_id(self, prefix: str = "id") -> str: ...
    async def send(self, content: str) -> Dict[Any, Any]: ...


class PoolQuery:
    

    def __init__(self, job: _SQLJobProtocol, query: str, opts: QueryOptions) -> None:
        self.job = job
        self.sql: str = query
        self.is_prepared: bool = True if opts.parameters is not None else False
        self.parameters: Optional[Union[Sequence[Any], Mapping[Union[str, int], Any]]] = opts.parameters
        self.is_cl_command: Optional[bool] = opts.isClCommand
        self.should_auto_close: Optional[bool] = opts.autoClose
        self.is_terse_results: Optional[bool] = opts.isTerseResults

        self._rows_to_fetch: int = 100
        self.state: QueryState = QueryState.NOT_YET_RUN
        self._correlation_id: Optional[str] = None

        

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def _execute_query(self, request: BaseRequest) -> Dict[Any, Any]:
        query_result = await self.job.send(json.dumps(dataclasses.asdict(request)))
        return query_result

    async def run(self, rows_to_fetch: Optional[int] = None) -> QueryResult:
        if rows_to_fetch is None:
            rows_to_fetch = self._rows_to_fetch
        else:
            self._rows_to_fetch = rows_to_fetch

        # check Query state first
        if self.state == QueryState.RUN_MORE_DATA_AVAIL:
            raise Exception("Statement has already been run")
        elif self.state == QueryState.RUN_DONE:
            raise Exception("Statement has already been fully run")

        logger.debug("Executing async query: rows_to_fetch=%s", rows_to_fetch)

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
            logger.error("Async query execution failed: %s", error_list)
            raise Exception(error_list)

        self._correlation_id = query_result.id
        logger.debug("Async query executed: correlation_id=%s done=%s", self._correlation_id, query_result.is_done)

        return query_result

    async def fetch_more(self, rows_to_fetch: Optional[int] = None) -> SqlMoreResponse:
        if rows_to_fetch is None:
            rows_to_fetch = self._rows_to_fetch
        else:
            self._rows_to_fetch = rows_to_fetch

        if self.state == QueryState.NOT_YET_RUN:
            raise Exception("Statement has not been run")
        elif self.state == QueryState.RUN_DONE:
            raise Exception("Statement has already been fully run")

        assert self._correlation_id is not None
        logger.debug("Fetching more rows: rows=%s correlation_id=%s", rows_to_fetch, self._correlation_id)
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
            logger.error("Async fetch failed: %s", query_result.error)
            raise Exception(query_result.error or "Failed to run Query (unknown error)")

        return query_result

    async def close(self):
        if not self.job.socket:
            raise Exception("SQL Job not connected")
        logger.debug("Closing async query: correlation_id=%s", self._correlation_id)
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
