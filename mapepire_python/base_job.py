import configparser
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

from .data_types import DaemonServer, JobStatus, QueryOptions, dict_to_dataclass


class BaseJob:
    def __init__(
        self,
        creds: Optional[Union[DaemonServer, Dict[str, Any], Path]] = None,
        options: Dict[Any, Any] = {},
        **kwargs,
    ) -> None:
        self.creds = creds
        self.options = options
        self.kwargs = kwargs

    def _parse_connection_input(
        self, db2_server: Union[DaemonServer, Dict[str, Any], Path], **kwargs: Any
    ) -> DaemonServer:
        if isinstance(db2_server, dict):
            db2_server = dict_to_dataclass(db2_server, DaemonServer)
        elif isinstance(db2_server, (str, Path)):
            config_path = Path(os.path.abspath(os.path.expanduser(db2_server)))
            if config_path.is_file():
                config = configparser.ConfigParser()
                config.read(config_path)
                system = kwargs.get("section", None)
                if system and system in config:
                    conn_settings = dict(config[system])
                else:
                    first_group = config.sections()[0]
                    conn_settings = dict(config[first_group])
                db2_server = dict_to_dataclass(conn_settings, DaemonServer)
            else:
                raise ValueError(f"The provided path '{db2_server}' is not a valid file.")

        if not isinstance(db2_server, DaemonServer):
            raise TypeError("db2_server must be of type DaemonServer")
        self.creds = db2_server
        return db2_server

    def __str__(self) -> str:
        creds_str = self.creds
        if isinstance(self.creds, DaemonServer):
            creds_dict = self.creds.__dict__.copy()
            creds_dict.pop("password", None)  # Remove password if present
            creds_str = str(creds_dict)
        return f"BaseJob(creds={creds_str}, options={self.options})"

    def connect(
        self, db2_server: Union[DaemonServer, Dict[str, Any], Path], **kwargs
    ) -> Dict[str, Any]:
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
