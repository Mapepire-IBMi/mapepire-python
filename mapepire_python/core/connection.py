from typing import Any, Dict, Sequence, Union

import pep249

from ..client.sql_job import SQLJob
from ..core.cursor import Cursor
from ..core.exceptions import convert_runtime_errors
from ..core.utils import raise_if_closed
from ..data_types import DaemonServer


class Connection(pep249.CursorExecuteMixin, pep249.ConcreteErrorMixin, pep249.Connection):
    @convert_runtime_errors
    def __init__(self, database: Union[DaemonServer, dict], opts={}) -> None:
        super().__init__()
        self.job = SQLJob(creds=database, options=opts)
        self.job.connect(database)
        self._closed = False

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

    def execute(
        self, operation: str, parameters: Sequence[Any] | Dict[str | int, Any] | None = None
    ) -> Cursor:
        return self.cursor().execute(operation, parameters)

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any] | Dict[str | int, Any]]
    ) -> Cursor:
        return self.cursor().executemany(operation, seq_of_parameters)

    def callproc(
        self, procname: str, parameters: Sequence[Any] | None = None
    ) -> Sequence[Any] | None:
        return self.cursor().callproc(procname, parameters)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass
