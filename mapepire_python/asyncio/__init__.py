from pathlib import Path
from typing import Any, Dict, Optional, Union

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
    connection_details: Optional[Union[DaemonServer, dict, Path]] = None,
    opts: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> AsyncConnection:
    """Connect to a Mapepire Server, returning an async connection.

    If connection_details is omitted, credentials are read from environment
    variables: MAPEPIRE_HOST, MAPEPIRE_USER, MAPEPIRE_PASSWORD, MAPEPIRE_PORT,
    and MAPEPIRE_CA_PATH.
    """
    if connection_details is None:
        connection_details = DaemonServer.from_env()
    return AsyncConnection(connection_details, opts, **kwargs)
