import base64
import ssl
from typing import Any, Dict

from websocket import WebSocket, create_connection

from ..data_types import DaemonServer


class WebsocketConnection:
    def __init__(self, db2_server: DaemonServer) -> None:
        self.uri = f"wss://{db2_server.host}:{db2_server.port}/db/"
        self.headers = {
            "Authorization": "Basic "
            + base64.b64encode(f"{db2_server.user}:{db2_server.password}".encode()).decode("ascii")
        }

        self.ssl_opts = self._build_ssl_options(db2_server)

    def _build_ssl_options(self, db2_server: DaemonServer) -> Dict[str, Any]:
        ssl_opts: Dict[str, Any] = {}
        if db2_server.ignoreUnauthorized:
            ssl_opts["cert_reqs"] = ssl.CERT_NONE
        if db2_server.ca:
            ssl_context = ssl.create_default_context(cadata=db2_server.ca)
            ssl_context.check_hostname = False
            ssl_opts["ssl_context"] = ssl_context
            ssl_opts["cert_reqs"] = ssl.CERT_NONE
        return ssl_opts

    def connect(self) -> WebSocket:
        try:
            return create_connection(
                self.uri, header=self.headers, sslopt=self.ssl_opts, timeout=10
            )
        except Exception as e:
            raise RuntimeError(f"An error occurred while connecting to the server: {e}")
