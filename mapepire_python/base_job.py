from typing import Any, Dict, Optional, Union

from mapepire_python.types import DaemonServer, JobStatus


class BaseJob:
    def __init__(self, creds: DaemonServer = None, options: Dict[Any, Any] = {}) -> None:
        self.creds = creds
        self.options = options

    async def connect(self, db2_server: Union[DaemonServer, Dict[str, Any]]) -> Dict[str, Any]:
        raise NotImplementedError()

    def connect(self, db2_server: Union[DaemonServer, Dict[str, Any]]) -> Dict[str, Any]:
        raise NotImplementedError()

    async def close(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def get_status(self) -> JobStatus:
        raise NotImplementedError()

    async def query_and_run(
        self, sql: str, opts: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Dict[str, Any]:
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
