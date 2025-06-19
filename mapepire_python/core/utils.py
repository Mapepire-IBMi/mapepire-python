"""Some useful utility pieces."""

from functools import wraps
from typing import Any, Callable, Dict, List, Optional

from .correlation_handler import CorrelationIDHandler
from .exceptions import CONNECTION_CLOSED, ProgrammingError, ReturnType
from .query_base import QueryResult

__all__ = ["raise_if_closed"]


def raise_if_closed(method: Callable[..., ReturnType]) -> Callable[..., ReturnType]:
    """
    Wrap a connection/cursor method and raise a 'connection closed' error if
    the object is closed.

    """

    @wraps(method)
    def wrapped(self, *args, **kwargs):
        """Raise if the connection/cursor is closed."""
        if self._closed:  # pylint: disable=protected-access
            raise CONNECTION_CLOSED
        return method(self, *args, **kwargs)

    return wrapped


def ignore_transaction_error(
    method: Callable[..., ReturnType],
) -> Callable[..., Optional[ReturnType]]:
    """
    Ignore transaction errors, returning `None` instead. Useful for
    `rollback`.

    """

    @wraps(method)
    def wrapped(*args, **kwargs):
        """Ignore transaction errors, returning `None` instead."""
        try:
            return method(*args, **kwargs)
        except (ProgrammingError, RuntimeError) as err:
            error_msg = str(err)
            if error_msg.endswith("no transaction is active"):
                return None
            
            # Check for correlation ID expiration using CorrelationIDHandler
            # Create a fake QueryResult to check if the error is correlation ID related
            fake_result = QueryResult({
                "success": False,
                "error": error_msg,
                "data": [],
                "is_done": False,
                "id": None
            })
            
            if CorrelationIDHandler.is_correlation_expired(fake_result):
                return None
            
            raise

    return wrapped


class ColumnMetaData:
    def __init__(
        self,
        name: str,
        type: str,
        display_size: int,
        label: str,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
        nullable: Optional[bool] = None,
        length: Optional[int] = None,
    ):
        self.name = name
        self.type = type
        self.display_size = display_size
        self.label = label
        self.precision = precision
        self.scale = scale
        self.nullable = nullable
        self.length = length


class MetaData:
    def __init__(self, column_count: int, job: str, columns: List[ColumnMetaData]):
        self.column_count = column_count
        self.job = job
        self.columns = columns


class QueryResultSet:
    def __init__(self, result: Dict[str, Any]):
        self.id = result.get("id", None)
        self.has_results = result.get("has_results", None)
        self.update_count = result.get("update_count", None)
        metadata = result.get("metadata", {})

        # Filter column metadata to only include known ColumnMetaData fields
        columns = []
        for col in metadata.get("columns", []):
            filtered_col = {
                "display_size": col.get("display_size", 0),
                "label": col.get("label", col.get("name", "UNKNOWN")),
                "name": col.get("name", "UNKNOWN"),
                "type": col.get("type", "VARCHAR"),
                "precision": col.get("precision"),
                "scale": col.get("scale"),
                "nullable": col.get("nullable"),
                "length": col.get("length"),
            }
            columns.append(ColumnMetaData(**filtered_col))

        self.metadata = MetaData(
            column_count=metadata.get("column_count", None),
            job=metadata.get("job", None),
            columns=columns,
        )
        self.data = result.get("data", [])
        self.is_done = result.get("is_done", None)
        self.success = result.get("success", None)
