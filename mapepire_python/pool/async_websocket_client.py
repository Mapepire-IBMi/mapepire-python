from websockets.asyncio.client import ClientConnection, connect

from mapepire_python.websocket import BaseConnection, handle_ws_errors


class AsyncWebSocketConnection(BaseConnection):
    def __init__(self, db2_server):
        super().__init__(db2_server)

    @handle_ws_errors
    async def connect(self) -> ClientConnection:
        websocket = await connect(
            self.uri,
            additional_headers=self.headers,
            open_timeout=10,
            ssl=self._create_ssl_context(self.db2_server),
        )
        return websocket
