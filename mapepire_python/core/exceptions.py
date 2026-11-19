"""
This module covers the exceptions outlined in PEP 249.

"""

# pylint: disable=missing-class-docstring
import ast
import inspect
import json
from functools import wraps
from typing import Callable, TypeVar

from pep249 import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

ReturnType = TypeVar("ReturnType")


__all__ = [
    "DatabaseError",
    "DataError",
    "Error",
    "InterfaceError",
    "IntegrityError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "CONNECTION_CLOSED",
    "convert_runtime_errors",
    "ReturnType",
]

CONNECTION_CLOSED = ProgrammingError("Cannot operate on a closed connection.")

SQLSTATE_CLASS_ERRORS = {
    "23": IntegrityError,
    **dict.fromkeys(("21", "22"), DataError),
    **dict.fromkeys(("24", "26", "34", "3F", "42"), ProgrammingError),
    **dict.fromkeys(("08", "25", "40", "57"), OperationalError),
    "0A": NotSupportedError,
    **dict.fromkeys(("38", "39", "58"), InternalError),
}


def _error_payload(error: RuntimeError) -> dict:
    """Return the server error dict carried by a query-layer `RuntimeError`, or `{}`."""
    if error.args and isinstance(error.args[0], dict):
        return error.args[0]
    for parse in (json.loads, ast.literal_eval):
        try:
            payload = parse(str(error))
        except (ValueError, SyntaxError):
            continue
        return payload if isinstance(payload, dict) else {}
    return {}


def _parse_runtime_error(error: RuntimeError) -> Error:
    """Map a query-layer `RuntimeError` onto the PEP 249 class for its SQLSTATE."""
    if isinstance(error, Error) or not isinstance(error, RuntimeError):
        return error
    payload = _error_payload(error)
    sqlstate = payload.get("sql_state") or None
    error_type = SQLSTATE_CLASS_ERRORS.get((sqlstate or "")[:2].upper(), DatabaseError)
    new_error = error_type(payload.get("error") or str(error))
    new_error.sqlstate = sqlstate
    new_error.sql_rc = payload.get("sql_rc") or None
    return new_error


def convert_runtime_errors(function: Callable[..., ReturnType]) -> Callable[..., ReturnType]:
    """Wrap a sync or async function, raising correct errors from `RuntimeError`s."""

    if inspect.iscoroutinefunction(function):

        @wraps(function)
        async def async_wrapper(*args, **kwargs):
            try:
                return await function(*args, **kwargs)
            except Error:
                raise
            except RuntimeError as err:
                raise _parse_runtime_error(err) from err

        return async_wrapper  # type: ignore[return-value]

    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Error:
            raise
        except RuntimeError as err:
            raise _parse_runtime_error(err) from err

    return wrapper
