from pathlib import Path
from typing import Any, Dict, Union

from ..asyncio.connection import AsyncConnection
from ..core.exceptions import (
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
from ..data_types import DaemonServer
from .cursor import AsyncCursor

__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "AsyncConnection",
    "AsyncCursor",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "IntegrityError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
]

# pylint: disable=invalid-name
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


def connect(
    connection_details: Union[DaemonServer, dict, Path], opts: Dict[str, Any] = {}, **kwargs
) -> AsyncConnection:
    """Connect to a Mapepire Server, returning a connection."""
    return AsyncConnection(connection_details, opts, **kwargs)
