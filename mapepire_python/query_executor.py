from typing import Any, Dict, Optional, Union

from mapepire_python.data_types import QueryOptions


class QueryExecutor:
    """Utility class for shared query building and validation logic"""

    @staticmethod
    def validate_query_options(opts: Any) -> None:
        """Validate query options parameter"""
        if opts is not None and not isinstance(opts, (dict, QueryOptions)):
            raise ValueError("opts must be a dictionary, a QueryOptions object, or None")

    @staticmethod
    def build_query_options(opts: Optional[Union[Dict[str, Any], QueryOptions]]) -> QueryOptions:
        """Build QueryOptions object from various input types"""
        if isinstance(opts, QueryOptions):
            return opts
        elif opts:
            return QueryOptions(**opts)
        else:
            return QueryOptions(isClCommand=False, parameters=None, autoClose=False)