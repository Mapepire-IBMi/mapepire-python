from websockets.sync.client import ClientConnection, connect

from mapepire_python.websocket import BaseConnection, handle_ws_errors

from ..data_types import DaemonServer


class WebsocketConnection(BaseConnection):

    def __init__(self, db2_server: DaemonServer) -> None:
        super().__init__(db2_server)

    @handle_ws_errors
    def connect(self) -> ClientConnection:
        return connect(
            self.uri,
            additional_headers=self.headers,
            open_timeout=10,
            ssl=self._create_ssl_context(self.db2_server),
        )
