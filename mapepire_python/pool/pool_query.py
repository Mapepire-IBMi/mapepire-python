from typing import Any, Dict, List, Optional

from mapepire_python.core.correlation_handler import CorrelationIDHandler
from mapepire_python.core.query_base import AsyncQueryExecutor, BaseQuery, QueryResult, QueryState
from mapepire_python.data_types import QueryOptions
from mapepire_python.pool.pool_job import PoolJob


class PoolQuery(BaseQuery[PoolJob]):
    """Asynchronous query implementation using the unified BaseQuery architecture."""
    
    global_query_list: List["PoolQuery"] = []

    def __init__(self, job: PoolJob, query: str, opts: QueryOptions) -> None:
        """Initialize asynchronous query with AsyncQueryExecutor."""
        executor = AsyncQueryExecutor()
        super().__init__(job, query, opts, executor)
        PoolQuery.global_query_list.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def run(self, rows_to_fetch: Optional[int] = None) -> QueryResult:
        """Execute the query and return results."""
        self._validate_query_state([QueryState.NOT_YET_RUN])
        
        if rows_to_fetch is not None:
            self._rows_to_fetch = rows_to_fetch
            
        query_object = self._build_query_object("run", rows_to_fetch)
        raw_result = await self.executor.execute_query(self.job, query_object)
        result = self._process_query_result(raw_result)
        
        self._handle_query_error(result)
        return result

    async def fetch_more(self, rows_to_fetch: Optional[int] = None) -> QueryResult:
        """Fetch more results from a previously executed query."""
        self._validate_query_state([QueryState.RUN_MORE_DATA_AVAIL])
        
        if rows_to_fetch is not None:
            self._rows_to_fetch = rows_to_fetch
            
        query_object = self._build_query_object("fetch_more", rows_to_fetch)
        raw_result = await self.executor.execute_query(self.job, query_object)
        result = self._process_query_result(raw_result, update_correlation_id=False)
        
        # Use CorrelationIDHandler to gracefully handle correlation ID expiration
        handled_result = CorrelationIDHandler.handle_fetch_result(result, self)
        
        # Check if this is a different result (correlation ID expiration was handled)
        if handled_result is not result:
            # Correlation ID was expired and handled - return the "done" result
            return handled_result
        
        # For the original result, check for actual errors
        if not handled_result.success:
            # This is a real error - set error state and raise
            self.state = QueryState.ERROR
            raise Exception(handled_result.error or "Failed to run Query (unknown error)")
            
        return handled_result

    async def close(self) -> Optional[QueryResult]:
        """Close the query and clean up resources."""
        if not self.executor.validate_connection(self.job):
            raise Exception("SQL Job not connected")
            
        if self._correlation_id and self.state != QueryState.RUN_DONE:
            self.state = QueryState.RUN_DONE
            query_object = self._build_query_object("close")
            raw_result = await self.executor.execute_query(self.job, query_object)
            return self._process_query_result(raw_result, update_correlation_id=False)
        elif not self._correlation_id:
            self.state = QueryState.RUN_DONE
            return None