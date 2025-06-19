"""
This module covers the exceptions outlined in PEP 249.

"""

# pylint: disable=missing-class-docstring
import ast
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

INTEGRITY_ERRORS = ("Constraint Error",)
PROGRAMMING_ERRORS = ("*FILE not found.",)
DATA_ERRORS = ("Invalid Input Error", "Out of Range Error")
INTERNAL_ERRORS = ()
OPERATIONAL_ERRORS = ()
NOT_SUPPORTED_ERRORS = ()


def _parse_runtime_error(error: RuntimeError) -> DatabaseError:
    """
    Parse a runtime error and return enhanced DatabaseError with user-friendly messages.

    Simple approach: Try to parse as dict, otherwise use as-is.
    """
    if isinstance(error, Error) or not isinstance(error, RuntimeError):
        return error

    error_string = str(error)

    # Try to parse as dictionary (most common case)
    try:
        error_dict = ast.literal_eval(error_string)
        if isinstance(error_dict, dict):
            return _create_enhanced_database_error(error_dict)
    except (ValueError, SyntaxError, TypeError):
        pass

    # Fallback: Create generic DatabaseError with cleaned message
    clean_message = error_string.replace("Database error: ", "").strip()
    return DatabaseError(clean_message)


def _create_enhanced_database_error(error_details: dict) -> DatabaseError:
    """
    Create enhanced DatabaseError with user-friendly message and structured information.

    Args:
        error_details: Dict containing error information from server response

    Returns:
        DatabaseError with enhanced message and additional attributes
    """
    # Extract core error information
    sql_state = error_details.get("sql_state", "")
    sql_code = error_details.get("sql_rc", 0)
    error_msg = error_details.get("error", "Unknown database error")

    # Build error message with original text
    message_parts = [error_msg]

    # Add diagnostic information for debugging
    diag_parts = []
    if sql_state:
        diag_parts.append(f"SQLSTATE={sql_state}")
    if sql_code:
        diag_parts.append(f"SQLCODE={sql_code}")

    if diag_parts:
        message_parts.append(f"({', '.join(diag_parts)})")

    # Create the final user-friendly message
    formatted_message = " ".join(message_parts)

    # Create enhanced DatabaseError
    db_error = DatabaseError(formatted_message)

    # Add attributes for programmatic access
    db_error.sql_state = sql_state
    db_error.sql_code = sql_code
    db_error.original_error = error_msg
    db_error.raw_details = error_details

    return db_error


def convert_runtime_errors(function: Callable[..., ReturnType]) -> Callable[..., ReturnType]:
    """Wrap a function, raising correct errors from `RuntimeError`s."""
    import inspect

    if inspect.iscoroutinefunction(function):
        # Async function - create async wrapper
        @wraps(function)
        async def async_wrapper(*args, **kwargs):
            try:
                return await function(*args, **kwargs)
            except RuntimeError as err:
                raise _parse_runtime_error(err) from err

        return async_wrapper
    else:
        # Sync function - create sync wrapper
        @wraps(function)
        def sync_wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except RuntimeError as err:
                raise _parse_runtime_error(err) from err

        return sync_wrapper
