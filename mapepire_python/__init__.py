from pathlib import Path
from typing import Any, Dict, Optional, Union

from .asyncio import connect as async_connect
from .asyncio.connection import AsyncConnection
from .client.query import QueryState
from .client.sql_job import SQLJob
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
from .core.exceptions import CONNECTION_CLOSED, convert_runtime_errors
from .data_types import DaemonServer, JobStatus, QueryOptions, QueryResult
from .pool.pool_client import Pool, PoolOptions
from .pool.pool_job import PoolJob
from .version import VERSION as __version__

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
    "DaemonServer",
    "SQLJob",
    "PoolJob",
    "Pool",
    "PoolOptions",
    "QueryOptions",
    "QueryResult",
    "QueryState",
    "JobStatus",
    "async_connect",
    "AsyncConnection",
]

# pylint: disable=invalid-name
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


def connect(
    connection_details: Optional[Union[DaemonServer, dict, Path]] = None,
    opts: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Connection:
    """Connect to a Mapepire Server, returning a connection.

    If connection_details is omitted, credentials are read from environment
    variables: MAPEPIRE_HOST, MAPEPIRE_USER, MAPEPIRE_PASSWORD, MAPEPIRE_PORT,
    and MAPEPIRE_CA_PATH.
    """
    if connection_details is None:
        connection_details = DaemonServer.from_env()
    return Connection(connection_details, opts, **kwargs)
