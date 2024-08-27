import asyncio
import base64
import json
import ssl
from typing import Any, Dict, Optional, Union

from websocket import WebSocket
import websockets
import websockets.asyncio
import websockets.asyncio.connection
from ..types import DaemonServer, JobStatus, QueryOptions
from ..client.websocket import WebsocketConnection

class PoolJob:
    unique_id_counter = 0

    def __init__(self, creds: DaemonServer = None, options: Optional[Dict[Any, Any]] = {}):
        self.creds = creds
        self.options = options
        self.socket = None
        self.response_emitter = {}
        self._status = JobStatus.NotStarted
        self.trace_file = None
        self.is_tracing_channel_data = False
        # self.unique_id = self.get_unique_id("sqljob")
        self.id = None
        self._unique_id_counter: int = 0
        self.requests = 0

    # @staticmethod
    # def get_new_unique_id(prefix: str = "id") -> str:
    #     PoolJob.unique_id_counter += 1
    #     return f"{prefix}{PoolJob.unique_id_counter}"

    def enable_local_trace(self):
        pass

    async def get_channel(self, db2_server: Dict[str, Any] | DaemonServer) -> websockets.WebSocketClientProtocol:
        uri = f"wss://{db2_server.host}:{db2_server.port}/db/"
        headers = {
            "Authorization": "Basic "
            + base64.b64encode(f"{db2_server.user}:{db2_server.password}".encode()).decode("ascii")
        }
        
        ssl_contest = ssl.create_default_context(cafile=db2_server.ca)
        ssl_contest.check_hostname = False
        ssl_contest.verify_mode = ssl.CERT_NONE
        
        socket = await websockets.connect(
            uri=uri, extra_headers=headers, ssl=ssl_contest, ping_timeout=None
        )
        
        return socket

    
    async def send(self, content: str, db2_server: DaemonServer) -> str:
        # sock = self.get_channel(db2_server)
        await self.socket.send(content)
        response = await self.socket.recv()
        return response 

    def get_status(self) -> str:
        pass

    def get_running_count(self) -> int:
        pass

    async def connect(self, db2_server: DaemonServer) -> Dict[str, Any]:
        self.socket = await self.get_channel(db2_server)
        props = ";".join(
            [
                f'{prop}={",".join(self.options[prop]) if isinstance(self.options[prop], list) else self.options[prop]}'
                for prop in self.options
            ]
        )

        connection_props = {
            "id": self.get_unique_id(),
            "type": "connect",
            "technique": "tcp",
            "application": "Python Client",
            "props": props if len(props) > 0 else "",
        }
        
        res = await self.send(json.dumps(connection_props), db2_server)
        try:
            result = json.loads(res)
        except Exception as e:
            print(f"an error occured while loading connect result: {e}")

        if result.get("success", False):
            self._status = JobStatus.Ready
        else:
            self._status = JobStatus.NotStarted
            print(result)
            raise Exception(result.get("error", "Failed to connect to server"))

        self.id = result["job"]
        self._is_tracing_channeldata = False

        return result
        

    def query(self, sql: str, opts: Optional[Dict[str, Any]] = None):
        pass

    async def execute(self, sql: str, opts: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        pass

    async def get_version(self) -> Dict[str, Any]:
        pass

    async def explain(self, statement: str, type: Optional[str] = "Run") -> Dict[str, Any]:
        pass

    def get_trace_file_path(self) -> Optional[str]:
        pass

    async def get_trace_data(self) -> Dict[str, Any]:
        pass

    async def set_trace_config(self, dest: str, level: str) -> Dict[str, Any]:
        pass

    def clcommand(self, cmd: str):
        pass

    def under_commit_control(self) -> bool:
        pass

    async def get_pending_transactions(self) -> int:
        pass

    async def end_transaction(self, type: str) -> Dict[str, Any]:
        pass

    def get_unique_id(self, prefix: str = "id") -> str:
        self._unique_id_counter += 1
        return f"{prefix}{self._unique_id_counter}"

    async def close(self):
        pass

    def dispose(self):
        pass

def url_to_daemon(uri: str) -> Dict[str, Any]:
    pass
        