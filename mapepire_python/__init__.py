from pathlib import Path
from typing import Any, Dict, Union

from .core import (
    Connection,
    Cursor,
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
from .data_types import DaemonServer

__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
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
    "connect",
    "Connection",
    "Cursor",
]

# pylint: disable=invalid-name
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


def connect(
    connection_details: Union[DaemonServer, dict, Path], opts: Dict[str, Any] = {}, **kwargs
) -> Connection:
    """Connect to a Mapepire Server, returning a connection."""
    return Connection(connection_details, opts, **kwargs)
