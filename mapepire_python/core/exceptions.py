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
    Parse a runtime error straight from DuckDB and return a more
    appropriate exception.

    """
    if isinstance(error, Error) or not isinstance(error, RuntimeError):
        return error
    error_string = str(error)
    error_message = None
    new_error_type = DatabaseError
    try:
        error_dict = ast.literal_eval(error_string)
        error_message = error_dict["error"]
        if any(err in error_message for err in PROGRAMMING_ERRORS):
            new_error_type = ProgrammingError
        return new_error_type(error_message)
    except Exception:
        pass

    # if error_type in INTEGRITY_ERRORS:
    #     new_error_type = IntegrityError
    # elif error_type in PROGRAMMING_ERRORS:
    #     new_error_type = ProgrammingError
    # elif error_type in DATA_ERRORS:
    #     new_error_type = DataError
    # elif error_type in INTERNAL_ERRORS:
    #     new_error_type = InternalError
    # elif error_type in OPERATIONAL_ERRORS:
    #     new_error_type = OperationalError
    # elif error_type in NOT_SUPPORTED_ERRORS:
    #     new_error_type = NotSupportedError

    return new_error_type(error_string)


def convert_runtime_errors(function: Callable[..., ReturnType]) -> Callable[..., ReturnType]:
    """Wrap a function, raising correct errors from `RuntimeError`s."""

    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except RuntimeError as err:
            raise _parse_runtime_error(err) from err

    return wrapper
