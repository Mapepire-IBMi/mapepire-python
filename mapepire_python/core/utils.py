"""Some useful utility pieces."""

from functools import wraps
from typing import Any, Callable, Dict, List, Optional

from .exceptions import CONNECTION_CLOSED, ProgrammingError, ReturnType

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
    method: Callable[..., ReturnType]
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
            if str(err).endswith("no transaction is active"):
                return None
            raise

    return wrapped


class ColumnMetaData:
    def __init__(self, name: str, type: str, display_size: int, label: str):
        self.name = name
        self.type = type
        self.display_size = display_size
        self.label = label


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
        self.metadata = MetaData(
            column_count=metadata.get("column_count", None),
            job=metadata.get("job", None),
            columns=[ColumnMetaData(**col) for col in metadata.get("columns", [])],
        )
        self.data = result.get("data", [])
        self.is_done = result.get("is_done", None)
        self.success = result.get("success", None)
