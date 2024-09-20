from pathlib import Path
from typing import Optional, Sequence, Union

import pep249
from pep249 import ProcArgs, QueryParameters, SQLQuery

from ..client.sql_job import SQLJob
from ..core.cursor import Cursor
from ..core.exceptions import convert_runtime_errors
from ..core.utils import raise_if_closed
from ..data_types import DaemonServer

__all__ = ["Connection"]

COMMIT = "COMMIT"
ROLLBACK = "ROLLBACK"


class Connection(pep249.CursorExecuteMixin, pep249.ConcreteErrorMixin, pep249.Connection):
    """
    A DB API 2.0 compliant connection for Mapepire, as outlined in
    PEP 249.

    Can be constructed by passing a connection details object as a dict,
    or a `DaemonServer` object:

    ```
    from mapepire_python import connect
    from mapepire_python.data_types import DaemonServer
    creds = DaemonServer(
        host=SERVER,
        port=PORT,
        user=USER,
        password=PASS,
        ignoreUnauthorized=True
    )
    with connect(creds) as conn:
        with conn.execute("select * from sample.employee") as cur:
            print(await cur.fetchone())

    ```
    """

    @convert_runtime_errors
    def __init__(self, database: Union[DaemonServer, dict, Path], opts={}, **kwargs) -> None:
        super().__init__()
        self._closed = False
        self.job = SQLJob(creds=database, options=opts, **kwargs)
        self.job.connect(database, **kwargs)

    @raise_if_closed
    @convert_runtime_errors
    def cursor(
        self,
    ) -> Cursor:
        return Cursor(self, self.job)

    @convert_runtime_errors
    def close(self) -> None:
        if self._closed:
            return

        self.job.close()
        self._closed = True

    def execute(self, operation: str, parameters: Optional[QueryParameters] = None) -> Cursor:
        return self.cursor().execute(operation, parameters)

    def executemany(self, operation: str, seq_of_parameters: Sequence[QueryParameters]) -> Cursor:
        return self.cursor().executemany(operation, seq_of_parameters)

    def callproc(self, procname: str, parameters: Optional[ProcArgs] = None) -> Optional[ProcArgs]:
        cursor = self.cursor()
        cursor.callproc(procname, parameters)
        return parameters

    def executescript(self, script: SQLQuery) -> Cursor:
        """A lazy implementation of SQLite's `executescript`."""
        return self.execute(script)

    def commit(self) -> None:
        self.job.query_and_run(COMMIT)

    def rollback(self) -> None:
        self.job.query_and_run(ROLLBACK)
