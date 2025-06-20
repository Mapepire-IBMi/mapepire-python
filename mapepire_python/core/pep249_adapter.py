"""
PEP 249 Query Adapter for bridging DB API 2.0 interface with unified BaseQuery architecture.

This module provides the adapter layer that connects PEP 249 cursor operations
with the unified query system, ensuring consistent behavior and eliminating duplication.
"""

from typing import Any, Dict, Optional, Union

from pep249 import QueryParameters

from ..client.query import Query
from ..pool.pool_query import PoolQuery
from .query_base import QueryResult, QueryState
from .query_factory import QueryFactory
from .result_processor import ResultSetProcessor


class PEP249QueryResult:
    """
    Wrapper for query results that provides PEP 249 specific formatting.

    This class bridges the gap between QueryResult objects and the expectations
    of PEP 249 cursor methods.
    """

    def __init__(self, query: Union[Query, PoolQuery], result: QueryResult):
        self.query = query
        self.result = result
        self.processor = ResultSetProcessor()

    @property
    def has_results(self) -> bool:
        """Check if the result contains data rows."""
        # Use server's has_results field if available, fallback to data check
        if 'has_results' in self.result:
            return self.result['has_results']
        return not self.processor.is_empty_result(self.result)

    @property
    def update_count(self) -> Optional[int]:
        """Get the number of affected rows for DML operations."""
        return self.result.metadata.get("update_count")

    @property
    def column_names(self) -> list[str]:
        """Get column names from the result."""
        return self.processor.extract_column_names(self.result)

    def get_formatted_result(self) -> Dict[str, Any]:
        """Get result formatted for PEP 249 usage."""
        return self.processor.process_execute_result(self.result)


class PEP249QueryAdapter:
    """
    Adapter to bridge PEP 249 interface with BaseQuery architecture.

    This adapter centralizes the interaction between cursor methods and the unified
    query system, providing consistent parameter handling and result processing.
    """

    def __init__(self, query_factory: QueryFactory):
        """
        Initialize the adapter with a query factory.

        Args:
            query_factory: Factory for creating queries
        """
        self.query_factory = query_factory
        self.current_query: Optional[Union[Query, PoolQuery]] = None
        self.processor = ResultSetProcessor()
        self._last_result: Optional[QueryResult] = None
        self._current_row_index: int = 0

    def execute_query(
        self, operation: str, parameters: Optional[QueryParameters] = None, **kwargs: Any
    ) -> PEP249QueryResult:
        """
        Execute query and return PEP 249 compatible result.

        Args:
            operation: SQL statement to execute
            parameters: Parameters for prepared statements
            **kwargs: Additional execution options

        Returns:
            PEP249QueryResult with formatted results

        Raises:
            RuntimeError: If query execution fails
        """
        # Create query using the factory for consistent options processing
        self.current_query = self.query_factory.create_pep249_query(operation, parameters, **kwargs)

        # Execute the query - handle both sync and async patterns
        try:
            if hasattr(self.current_query, "prepare_sql_execute"):
                # Sync query with parameters
                result = self.current_query.prepare_sql_execute()
            else:
                # Standard run method
                result = self.current_query.run()

            self._last_result = result
            self._current_row_index = 0  # Reset row index for new query
            return PEP249QueryResult(self.current_query, result)

        except Exception as e:
            raise RuntimeError(e)

    def fetch_results(self, rows_to_fetch: Optional[int] = None) -> Optional[QueryResult]:
        """
        Fetch results from current query.

        Args:
            rows_to_fetch: Number of rows to fetch (None for default)

        Returns:
            QueryResult with fetched data or None if no active query

        Raises:
            RuntimeError: If no query is active or fetch fails
        """
        if not self.current_query:
            raise RuntimeError("No active query to fetch from")

        if self.current_query.state == QueryState.NOT_YET_RUN:
            raise RuntimeError("Query has not been executed yet")

        if self.current_query.state == QueryState.RUN_DONE:
            # No more data available
            return None

        if self.current_query.state == QueryState.ERROR:
            raise RuntimeError("Query is in error state")

        try:
            result = self.current_query.fetch_more(rows_to_fetch)
            self._last_result = result
            return result
        except Exception as e:
            raise RuntimeError(e)

    def get_fetchone_result(self) -> Optional[Dict[str, Any]]:
        """
        Get single row result for cursor.fetchone().

        Returns:
            Single row as dictionary or None if no data
        """
        if not self.current_query:
            return None

        # If query is in error state, return None
        if self.current_query.state == QueryState.ERROR:
            return None

        # If query hasn't been executed yet, return None
        if self.current_query.state == QueryState.NOT_YET_RUN:
            return None

        # First, check if we have data from the current result to iterate through
        if self._last_result and self._last_result.data:
            data = self._last_result.data
            if self._current_row_index < len(data):
                row = data[self._current_row_index]
                self._current_row_index += 1
                return row
            # If we've exhausted current data, handle based on query state
            else:
                # If query is done and we've consumed all data, return None
                if self.current_query.state == QueryState.RUN_DONE:
                    return None
                # If more data is available, reset index and continue to fetch_more logic
                else:
                    self._current_row_index = 0

        # For queries with more data available, try to fetch it
        if self.current_query.state == QueryState.RUN_MORE_DATA_AVAIL:
            try:
                result = self.fetch_results(rows_to_fetch=1)
                if not result or not result.data:
                    return None

                # Store the new result and reset row index
                self._last_result = result
                self._current_row_index = 0
                
                # Return the first row from the new result
                if self._last_result.data:
                    row = self._last_result.data[self._current_row_index]
                    self._current_row_index += 1
                    return row

            except RuntimeError as e:
                # The fetch_more method already handles correlation ID expiration using CorrelationIDHandler
                # If we get an exception here, it's a real error, so re-raise it
                raise

        return None

    def get_fetchmany_result(self, size: int = 1) -> Dict[str, Any]:
        """
        Get multiple rows result for cursor.fetchmany().

        Args:
            size: Number of rows to fetch

        Returns:
            Dictionary with QueryResult format
        """
        try:
            result = self.fetch_results(rows_to_fetch=size)
            if not result:
                # Return empty result structure
                empty_result = QueryResult(
                    {"success": True, "data": [], "is_done": True, "id": None}
                )
                return self.processor.process_fetchmany_result(empty_result, size)

            return self.processor.process_fetchmany_result(result, size)
        except RuntimeError as e:
            # The fetch_more method already handles correlation ID expiration using CorrelationIDHandler
            # If we get an exception here, it's a real error, so re-raise it
            raise

    def get_fetchall_result(self) -> Dict[str, Any]:
        """
        Get all remaining rows for cursor.fetchall().

        Returns:
            Dictionary with QueryResult format containing all remaining data
        """
        # For fetchall, we need to fetch all remaining data
        all_data = []

        while self.current_query and self.current_query.state == QueryState.RUN_MORE_DATA_AVAIL:
            result = self.fetch_results(rows_to_fetch=100)  # Fetch in chunks
            if not result or not result.data:
                break
            all_data.extend(result.data)

            if result.is_done:
                break

        # Create a composite result
        if self._last_result:
            composite_result = QueryResult(
                {
                    "success": True,
                    "data": all_data,
                    "is_done": True,
                    "id": self._last_result.correlation_id,
                    "metadata": self._last_result.metadata,
                }
            )
        else:
            composite_result = QueryResult(
                {"success": True, "data": all_data, "is_done": True, "id": None}
            )

        return self.processor.process_fetchall_result(composite_result)

    def close_query(self) -> None:
        """Close the current query and clean up resources."""
        if self.current_query:
            try:
                self.current_query.close()
            except Exception:
                # Ignore errors during cleanup
                pass
            finally:
                self.current_query = None
                self._last_result = None
                self._current_row_index = 0

    def is_query_active(self) -> bool:
        """Check if there's an active query."""
        return self.current_query is not None

    def get_query_state(self) -> Optional[QueryState]:
        """Get the state of the current query."""
        return self.current_query.state if self.current_query else None

    def has_more_data(self) -> bool:
        """Check if the current query has more data available."""
        if not self.current_query:
            return False
        return self.current_query.state == QueryState.RUN_MORE_DATA_AVAIL
