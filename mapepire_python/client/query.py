from typing import Any, Dict, List, Optional

from mapepire_python.websocket import handle_ws_errors

from ..core.query_base import BaseQuery, QueryResult, QueryState, SyncQueryExecutor
from ..data_types import QueryOptions
from .sql_job import SQLJob


class Query(BaseQuery[SQLJob]):
    """Synchronous query implementation using the unified BaseQuery architecture."""
    
    global_query_list: List["Query"] = []

    def __init__(self, job: SQLJob, query: str, opts: QueryOptions) -> None:
        """Initialize synchronous query with SyncQueryExecutor."""
        executor = SyncQueryExecutor()
        super().__init__(job, query, opts, executor)
        Query.global_query_list.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @handle_ws_errors
    def prepare_sql_execute(self) -> QueryResult:
        """Prepare and execute SQL with parameters."""
        self._validate_query_state([QueryState.NOT_YET_RUN])
        
        query_object = self._build_query_object("prepare_execute")
        raw_result = self.executor.execute_query(self.job, query_object)
        result = self._process_query_result(raw_result)
        
        self._handle_query_error(result)
        return result

    @handle_ws_errors
    def run(self, rows_to_fetch: Optional[int] = None) -> QueryResult:
        """Execute the query and return results."""
        self._validate_query_state([QueryState.NOT_YET_RUN])
        
        if rows_to_fetch is not None:
            self._rows_to_fetch = rows_to_fetch
            
        query_object = self._build_query_object("run", rows_to_fetch)
        raw_result = self.executor.execute_query(self.job, query_object)
        result = self._process_query_result(raw_result)
        
        self._handle_query_error(result)
        return result

    @handle_ws_errors 
    def fetch_more(self, rows_to_fetch: Optional[int] = None) -> QueryResult:
        """Fetch more results from a previously executed query."""
        self._validate_query_state([QueryState.RUN_MORE_DATA_AVAIL])
        
        if rows_to_fetch is not None:
            self._rows_to_fetch = rows_to_fetch
            
        query_object = self._build_query_object("fetch_more", rows_to_fetch)
        raw_result = self.executor.execute_query(self.job, query_object)
        result = self._process_query_result(raw_result)
        
        if not result.success:
            # Handle server incorrectly reporting more data available
            error_msg = result.error or ""
            if "invalid correlation ID" in error_msg.lower() or "bad request" in error_msg.lower():
                # Server says no more data available, update state and return empty result
                self.state = QueryState.RUN_DONE
                return QueryResult({
                    "success": True,
                    "data": [],
                    "is_done": True,
                    "id": result.correlation_id,
                    "metadata": {}
                })
            
            # For other errors, set error state and raise
            self.state = QueryState.ERROR
            raise RuntimeError(result.error or "Failed to run Query (unknown error)")
            
        return result

    @handle_ws_errors
    def close(self) -> Optional[QueryResult]:
        """Close the query and clean up resources."""
        if not self.executor.validate_connection(self.job):
            raise Exception("SQL Job not connected")
            
        if self._correlation_id and self.state != QueryState.RUN_DONE:
            self.state = QueryState.RUN_DONE
            query_object = self._build_query_object("close")
            raw_result = self.executor.execute_query(self.job, query_object)
            return self._process_query_result(raw_result)
        elif not self._correlation_id:
            self.state = QueryState.RUN_DONE
            return None