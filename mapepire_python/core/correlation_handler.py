"""
Correlation ID Error Handling

Provides standardized, graceful handling of IBM i server correlation ID
lifecycle issues across all query types.
"""

import re
from typing import Optional, Union
from .query_base import QueryResult, QueryState


class CorrelationIDHandler:
    """
    Standardized handler for correlation ID lifecycle management.
    
    The IBM i server sometimes cleans up query contexts while reporting
    is_done=false. This handler provides consistent detection and graceful
    handling of correlation ID expiration across all query types.
    """
    
    # Patterns that indicate correlation ID has expired/become invalid
    CORRELATION_ERROR_PATTERNS = [
        r"invalid correlation id",
        r"correlation id.*not found", 
        r"correlation id.*invalid",
        r"bad request",
        r"no transaction is active",
        r"cursor.*closed",
        r"query.*expired"
    ]
    
    @classmethod
    def is_correlation_expired(cls, result: QueryResult) -> bool:
        """
        Check if the result indicates correlation ID has expired.
        
        Args:
            result: QueryResult from server
            
        Returns:
            True if correlation ID has expired and query should be marked done
        """
        if result.success:
            return False
            
        error_msg = (result.error or "").lower()
        
        for pattern in cls.CORRELATION_ERROR_PATTERNS:
            if re.search(pattern, error_msg):
                return True
        
        return False
    
    @classmethod
    def handle_fetch_result(cls, result: QueryResult, query_obj) -> QueryResult:
        """
        Handle fetch_more result, checking for correlation ID expiration.
        
        Args:
            result: QueryResult from fetch_more operation
            query_obj: Query object with state to update
            
        Returns:
            QueryResult - either the original result or empty "done" result
        """
        if result.success:
            # Normal successful fetch
            return result
            
        if cls.is_correlation_expired(result):
            # Correlation ID expired - this is normal end of query, not an error
            query_obj.state = QueryState.RUN_DONE
            return cls.create_done_result(result.correlation_id)
            
        # Actual error - return as-is to be handled by error handling decorators  
        return result
    
    @classmethod
    def create_done_result(cls, correlation_id: Optional[str] = None) -> QueryResult:
        """
        Create a QueryResult indicating the query is done with no more data.
        
        Args:
            correlation_id: Optional correlation ID for the result
            
        Returns:
            QueryResult indicating successful completion with no data
        """
        return QueryResult({
            "success": True,
            "data": [],
            "is_done": True,
            "id": correlation_id,
            "metadata": {},
            "has_results": False
        })
    
    @classmethod 
    def should_continue_fetching(cls, query_obj) -> bool:
        """
        Check if query should continue fetching more data.
        
        Args:
            query_obj: Query object to check
            
        Returns:
            True if more data should be fetched
        """
        return (
            hasattr(query_obj, 'state') and 
            query_obj.state == QueryState.RUN_MORE_DATA_AVAIL
        )


# Convenience functions for backward compatibility and ease of use
def is_correlation_expired(result: QueryResult) -> bool:
    """Check if result indicates correlation ID has expired."""
    return CorrelationIDHandler.is_correlation_expired(result)


def handle_fetch_result(result: QueryResult, query_obj) -> QueryResult:
    """Handle fetch result with correlation ID expiration check.""" 
    return CorrelationIDHandler.handle_fetch_result(result, query_obj)


def create_done_result(correlation_id: Optional[str] = None) -> QueryResult:
    """Create a 'done' result for query completion."""
    return CorrelationIDHandler.create_done_result(correlation_id)