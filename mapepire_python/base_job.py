from typing import Any, Dict, Optional, Union

from .data_types import DaemonServer, JobStatus, QueryOptions


class BaseJob:
    def __init__(self, creds: DaemonServer = None, options: Dict[Any, Any] = {}) -> None:
        self.creds = creds
        self.options = options

    def connect(self, db2_server: Union[DaemonServer, Dict[str, Any]]) -> Dict[str, Any]:
        raise NotImplementedError()

    def close(self) -> None:
        raise NotImplementedError()

    def get_status(self) -> JobStatus:
        raise NotImplementedError()

    def query(
        self,
        sql: str,
        opts: Optional[Union[Dict[str, Any], QueryOptions]] = None,
    ):
        raise NotImplementedError()

    def query_and_run(
        self, sql: str, opts: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Dict[str, Any]:
        raise NotImplementedError()

    def __enter__(self):
        raise NotImplementedError()

    def __exit__(self, *args, **kwargs):
        raise NotImplementedError()

    async def __aenter__(self):
        raise NotImplementedError()

    async def __aexit__(self, *args, **kwargs):
        raise NotImplementedError()
