import base64
import json
import ssl
from typing import Any, Dict, Optional

from websocket import WebSocket, create_connection

from python_wsdb.types import DaemonServer, JobStatus, QueryOptions


class SQLJob:
    def __init__(self, options: Dict[Any, Any] = {}) -> None:
        self.options = options
        self._unique_id_counter: int = 0
        self._reponse_emitter = None
        self._status: JobStatus = JobStatus.NotStarted
        self._trace_file = None
        self._is_tracing_channeldata: bool = True

        self.__unique_id = self._get_unique_id("sqljob")
        self.id: Optional[str] = None

    def _get_unique_id(self, prefix: str = "id") -> str:
        self._unique_id_counter += 1
        return f"{prefix}{self._unique_id_counter}"

    def _get_channel(self, db2_server: DaemonServer) -> WebSocket:
        uri = f"wss://{db2_server.host}:{db2_server.port}/db/"
        headers = {
            "Authorization": "Basic "
            + base64.b64encode(f"{db2_server.user}:{db2_server.password}".encode()).decode("ascii")
        }

        # Prepare SSL context if necessary
        ssl_opts: Dict[str, Any] = {}

        if db2_server.ignoreUnauthorized:
            ssl_opts["cert_reqs"] = ssl.CERT_NONE
        if db2_server.ca:
            ssl_context = ssl.create_default_context(cadata=db2_server.ca)
            ssl_context.check_hostname = False
            ssl_opts["ssl_context"] = ssl_context
            ssl_opts["cert_reqs"] = ssl.CERT_NONE  # ignore certs for now

        # Create WebSocket connection
        socket = create_connection(uri, header=headers, sslopt=ssl_opts)

        # Register message handler
        def on_message(ws, message):
            if self._is_tracing_channeldata:
                print(message)
            try:
                response = json.loads(message)
                print(f"Received message with ID: {response['id']}")
            except Exception as e:
                print(f"Error parsing message: {e}")

        socket.on_message = on_message

        return socket

    def send(self, content):
        self._socket.send(content)

    def connect(self, db2_server: DaemonServer) -> Dict[Any, Any]:
        self._socket: WebSocket = self._get_channel(db2_server)

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
            raise Exception(result.get("error", "Failed to connect to server"))

        self.id = result["job"]
        self._is_tracing_channeldata = False

        return result

    def query(
        self, sql: str, opts: Optional[Dict[str, Any]] = None,
    ):
        from python_wsdb.client.query import Query

        if isinstance(opts, dict):
            opts = QueryOptions(**opts)
            return Query(job=self, query=sql, opts=opts)
        return Query(job=self, query=sql)

    def query_and_run(
        self, sql: str, opts: Optional[Dict[str, Any]] = None, **kwargs
    ):
        query = self.query(sql, opts)
        return query.run(**kwargs)

    def close(self):
        self._socket.close()
