"""Some useful utility pieces."""

import dataclasses
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, cast

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
            if str(err).endswith("no transaction is active"):
                return None
            raise

    return wrapped


class ColumnMetaData:
    def __init__(self, name: str, type: str, display_size: int, label: str,
                 precision=None, scale=None, nullable=None, **kwargs):
        self.name = name
        self.type = type
        self.display_size = display_size
        self.label = label
        self.precision = precision
        self.scale = scale
        self.nullable = nullable


class MetaData:
    def __init__(self, column_count: int, job: str, columns: List[ColumnMetaData]):
        self.column_count = column_count
        self.job = job
        self.columns = columns


class QueryResultSet:
    def __init__(self, result) -> None:
        if dataclasses.is_dataclass(result) and not isinstance(result, type):
            result = dataclasses.asdict(result)
        result = cast(Dict[str, Any], result)
        self.id = result.get("id", None)
        self.has_results = result.get("has_results", None)
        self.update_count = result.get("update_count", None)
        metadata = cast(Dict[str, Any], result.get("metadata") or {})
        self.metadata = MetaData(
            column_count=metadata.get("column_count", 0),
            job=metadata.get("job", ""),
            columns=[ColumnMetaData(**col) for col in metadata.get("columns", [])],  # type: ignore[arg-type]
        )
        self.data = result.get("data", [])
        self.is_done = result.get("is_done", None)
        self.success = result.get("success", None)
