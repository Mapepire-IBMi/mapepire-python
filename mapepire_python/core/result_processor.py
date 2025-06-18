"""
Result set processor for unified result handling across query types.

This module provides centralized result processing logic that ensures consistent
formatting and behavior across PEP 249 methods and direct query usage.
"""

from typing import Any, Dict, List, Optional, Sequence
from .query_base import QueryResult


class ResultSetProcessor:
    """
    Centralized result set processing for consistent formatting.
    
    This class provides unified result processing methods that handle the conversion
    between QueryResult objects and the various formats expected by different
    parts of the system (PEP 249, direct query usage, etc.).
    """

    @staticmethod
    def process_fetchone_result(result: QueryResult) -> Optional[Dict[str, Any]]:
        """
        Process result for fetchone() - returns single row or None.
        
        Args:
            result: QueryResult from query execution
            
        Returns:
            Single row as dictionary or None if no data
        """
        if not result.success or not result.data:
            return None
        return result.data[0] if result.data else None

    @staticmethod
    def process_fetchmany_result(result: QueryResult, size: int = 1) -> Dict[str, Any]:
        """
        Process result for fetchmany() - returns QueryResult dict with limited rows.
        
        Args:
            result: QueryResult from query execution
            size: Maximum number of rows to return
            
        Returns:
            Dictionary with QueryResult format containing limited data
        """
        # Limit data to requested size
        limited_data = result.data[:size] if result.data else []
        
        return {
            "success": result.success,
            "data": limited_data,
            "has_results": len(limited_data) > 0,
            "is_done": result.is_done and len(result.data) <= size,
            "metadata": result.metadata,
            "id": result.correlation_id,
            "error": result.error,
            "sql_state": result.sql_state,
            "sql_rc": result.sql_rc,
            "execution_time": result.get("execution_time", 0)
        }

    @staticmethod
    def process_fetchall_result(result: QueryResult) -> Dict[str, Any]:
        """
        Process result for fetchall() - returns complete QueryResult dict.
        
        Args:
            result: QueryResult from query execution
            
        Returns:
            Complete QueryResult dictionary format
        """
        return {
            "success": result.success,
            "data": result.data,
            "has_results": len(result.data) > 0,
            "is_done": result.is_done,
            "metadata": result.metadata,
            "id": result.correlation_id,
            "error": result.error,
            "sql_state": result.sql_state,
            "sql_rc": result.sql_rc,
            "execution_time": result.get("execution_time", 0)
        }

    @staticmethod
    def process_execute_result(result: QueryResult) -> Dict[str, Any]:
        """
        Process result for execute() - returns result with metadata for cursor operations.
        
        Args:
            result: QueryResult from query execution
            
        Returns:
            Dictionary formatted for cursor.execute() usage
        """
        processed = ResultSetProcessor.process_fetchall_result(result)
        
        # Add PEP 249 specific fields
        processed["update_count"] = result.metadata.get("update_count", None)
        processed["has_results"] = len(result.data) > 0
        
        return processed

    @staticmethod
    def extract_column_names(result: QueryResult) -> List[str]:
        """
        Extract column names from query result metadata.
        
        Args:
            result: QueryResult containing metadata
            
        Returns:
            List of column names or empty list if no metadata
        """
        if not result.metadata or "columns" not in result.metadata:
            # Fallback: extract from first data row if available
            if result.data and len(result.data) > 0:
                return list(result.data[0].keys())
            return []
        
        return [col["name"] for col in result.metadata["columns"]]

    @staticmethod
    def format_error_details(result: QueryResult) -> Dict[str, Any]:
        """
        Format error information from QueryResult for consistent error handling.
        
        Args:
            result: QueryResult that may contain error information
            
        Returns:
            Dictionary with standardized error details
        """
        error_details = {}
        
        if result.error:
            error_details["error"] = result.error
        if result.sql_state:
            error_details["sql_state"] = result.sql_state
        if result.sql_rc is not None:
            error_details["sql_rc"] = result.sql_rc
            
        if not error_details and not result.success:
            error_details["error"] = "Query execution failed for unknown reason"
            
        return error_details

    @staticmethod
    def is_empty_result(result: QueryResult) -> bool:
        """
        Check if the result represents an empty result set.
        
        Args:
            result: QueryResult to check
            
        Returns:
            True if result is empty, False otherwise
        """
        return not result.data or len(result.data) == 0

    @staticmethod
    def get_row_count(result: QueryResult) -> int:
        """
        Get the number of rows in the result set.
        
        Args:
            result: QueryResult to count
            
        Returns:
            Number of rows in the result
        """
        return len(result.data) if result.data else 0