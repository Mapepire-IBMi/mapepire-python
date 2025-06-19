"""
Query factory for centralized query creation and options processing.

This module provides a unified interface for creating queries with consistent
parameter handling and options processing across sync and async operations.
"""

from typing import Any, Optional, Union

from pep249 import QueryParameters

from ..client.query import Query
from ..client.sql_job import SQLJob
from ..data_types import QueryOptions
from ..pool.pool_job import PoolJob
from ..pool.pool_query import PoolQuery
from ..query_manager import QueryManager
from .parameter_parser import ParameterParser


class QueryFactory:
    """
    Factory for creating queries with unified options processing.

    This factory centralizes query creation logic and eliminates direct Query instantiation
    throughout the codebase, providing a consistent interface for both sync and async operations.
    """

    def __init__(self, job: Union[SQLJob, PoolJob]) -> None:
        """Initialize the factory with a job instance."""
        self.job = job
        self.query_manager = QueryManager(job)

    def create_query(
        self, operation: str, parameters: Optional[QueryParameters] = None, **kwargs: Any
    ) -> Union[Query, PoolQuery]:
        """
        Create query with unified options processing.

        Args:
            operation: SQL statement or CL command to execute
            parameters: Parameters for prepared statements
            **kwargs: Additional query options (isClCommand, isTerseResults, etc.)

        Returns:
            Query or PoolQuery instance based on job type
        """
        opts = self._build_query_options(parameters, **kwargs)
        return self.query_manager.create_query(operation, opts)

    def create_pep249_query(
        self, operation: str, parameters: Optional[QueryParameters] = None, **kwargs: Any
    ) -> Union[Query, PoolQuery]:
        """
        Create query specifically for PEP 249 compatibility.

        This method ensures query options are configured appropriately for
        PEP 249 cursor operations.
        """
        # Set PEP 249 specific defaults
        pep249_kwargs = {
            "isClCommand": False,  # PEP 249 is SQL-focused
            "autoClose": False,  # Let cursor manage lifecycle
            "isTerseResults": False,  # Standard format for compatibility
            **kwargs,  # Allow overrides
        }

        return self.create_query(operation, parameters, **pep249_kwargs)

    def _build_query_options(
        self, parameters: Optional[QueryParameters], **kwargs: Any
    ) -> QueryOptions:
        """
        Centralized query options building.

        This method provides consistent parameter processing across all query types,
        eliminating duplication in option handling.
        """
        # Use centralized parameter parser for consistent conversion
        converted_parameters = ParameterParser.parse_single_parameter_set(parameters)

        return QueryOptions(
            isClCommand=kwargs.get("isClCommand"),
            isTerseResults=kwargs.get("isTerseResults"),
            parameters=converted_parameters,
            autoClose=kwargs.get("autoClose"),
        )

    def get_job_type(self) -> str:
        """Get the type of job this factory creates queries for."""
        return "sync" if isinstance(self.job, SQLJob) else "async"

    def is_connected(self) -> bool:
        """Check if the underlying job is connected."""
        if isinstance(self.job, SQLJob):
            return self.job._socket is not None
        else:  # PoolJob
            return self.job.socket is not None
