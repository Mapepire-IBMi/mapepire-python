import json
from pathlib import Path
from typing import Any, Dict, Optional, Union

from websockets.sync.client import ClientConnection

from mapepire_python.client.websocket_client import WebsocketConnection
from mapepire_python.websocket import handle_ws_errors

from ..base_job import BaseJob
from ..data_types import DaemonServer, JobStatus, QueryOptions

__all__ = ["SQLJob"]


class SQLJob(BaseJob):
    def __init__(
        self,
        creds: Optional[Union[DaemonServer, Dict[str, Any], Path]] = None,
        options: Dict[Any, Any] = {},
        **kwargs,
    ) -> None:
        super().__init__(creds, options, **kwargs)
        self._socket = None
        self._unique_id_counter: int = 0
        self._reponse_emitter = {}
        self._status: JobStatus = JobStatus.NotStarted
        self._trace_file = None
        self._is_tracing_channeldata: bool = True

        self.__unique_id = self._get_unique_id("sqljob")
        self.id: Optional[str] = None

    def __enter__(self):
        if self.creds:
            self.connect(self.creds, **self.kwargs)
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def _get_unique_id(self, prefix: str = "id") -> str:
        """returns a unique id for the job object with the given prefix

        Args:
            prefix (str, optional): _description_. Defaults to "id".

        Returns:
            str: unique id
        """
        self._unique_id_counter += 1
        return f"{prefix}{self._unique_id_counter}"

    def _get_channel(self, db2_server: DaemonServer) -> ClientConnection:
        """returns a websocket connection to the mapepire server

        Args:
            db2_server (DaemonServer): _description_

        Returns:
            WebSocket: websocket connection
        """
        socket = WebsocketConnection(db2_server)
        return socket.connect()

    def get_status(self) -> JobStatus:
        """returns the current status of the job

        Returns:
            JobStatus: job status
        """
        return self._status

    @handle_ws_errors
    def send(self, content: str) -> None:
        """sends content to the mapepire server

        Args:
            content (str): JSON content to be sent
        """
        self._socket.send(content)

    @handle_ws_errors
    def connect(
        self, db2_server: Union[DaemonServer, Dict[str, Any], Path], **kwargs
    ) -> Dict[Any, Any]:
        """create connection to the mapepire server

        Args:
            db2_server (Union[DaemonServer, Dict[str, Any]]): server credentials

        Raises:
            Exception: Failed to connect to server

        Returns:
            Dict[Any, Any]: connection results from the server
        """
        db2_server = self._parse_connection_input(db2_server, **kwargs)

        self._socket: ClientConnection = self._get_channel(db2_server)

        props = ";".join(
            [
                f'{prop}={",".join(self.options[prop]) if isinstance(self.options[prop], list) else self.options[prop]}'
                for prop in self.options
            ]
        )

        connection_props = {
            "id": self._get_unique_id(),
            "type": "connect",
            "technique": "tcp",
            "application": "Python Client",
            "props": props if len(props) > 0 else "",
        }

        self.send(json.dumps(connection_props))
        result: Dict[str, Any] = {}
        try:
            result = json.loads(self._socket.recv())
        except Exception as e:
            print(f"an error occured while loading connect result: {e}")

        if result.get("success", False):
            self._status = JobStatus.Ready
        else:
            self._status = JobStatus.NotStarted
            self.close()
            raise Exception(result.get("error", "Failed to connect to server"))

        self.id = result["job"]
        self._is_tracing_channeldata = False

        return result

    def query(
        self,
        sql: str,
        opts: Optional[Union[Dict[str, Any], QueryOptions]] = None,
    ):
        """
        Create a Query object using provided SQL and options. If opts is None,
        the default options defined in Query constructor are used. opts can be a
        dictionary to be converted to QueryOptions, or a QueryOptions object directly.

        Args:
        sql (str): The SQL query string.
        opts (Optional[Union[Dict[str, Any], QueryOptions]]): Additional options
                for the query which can be a dictionary or a QueryOptions object.

        Returns:
        Query: A configured Query object.
        """
        from .query import Query

        if opts is not None and not isinstance(opts, (dict, QueryOptions)):
            raise ValueError("opts must be a dictionary, a QueryOptions object, or None")

        query_options = (
            opts
            if isinstance(opts, QueryOptions)
            else (
                QueryOptions(**opts)
                if opts
                else QueryOptions(isClCommand=False, parameters=None, autoClose=False)
            )
        )

        return Query(job=self, query=sql, opts=query_options)

    @handle_ws_errors
    def query_and_run(
        self, sql: str, opts: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Dict[str, Any]:
        """Create a Query object using provided SQL and options, then run the query.

        Args:
            sql (str): query string
            opts (Optional[Dict[str, Any]], optional): query options. Defaults to None.

        Raises:
            RuntimeError: Failed to run query

        Returns:
            Dict[str, Any]: query results
        """
        try:
            with self.query(sql, opts) as query:
                return query.run(**kwargs)
        except Exception as e:
            raise RuntimeError(f"Failed to run query: {e}")

    def close(self) -> None:
        self._status = JobStatus.Ended
        if self._socket:
            self._socket.close()
