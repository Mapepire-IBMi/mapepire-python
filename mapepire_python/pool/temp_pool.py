import asyncio
import base64
import json
import ssl
from enum import Enum
from typing import Dict, Any, Optional

import websockets
from pyee.asyncio import AsyncIOEventEmitter

DEFAULT_PORT = 8080  # Assuming this is the default port, adjust if needed

class JobStatus(Enum):
    NotStarted = "NotStarted"
    Connecting = "Connecting"
    Ready = "Ready"
    Busy = "Busy"

class SQLJob:
    unique_id_counter = 0

    @staticmethod
    def get_new_unique_id(prefix: str = "id") -> str:
        SQLJob.unique_id_counter += 1
        return f"{prefix}{SQLJob.unique_id_counter}"

    def __init__(self, options: Dict[str, Any] = None):
        self.options = options or {}
        self.websocket = None
        self.status = JobStatus.NotStarted
        self.response_emitter = AsyncIOEventEmitter()
        self.is_tracing_channel_data = False
        self.unique_id = self.get_new_unique_id("sqljob")
        self.id = None

    def enable_local_trace(self):
        self.is_tracing_channel_data = True

    async def get_channel(self, db2_server: Dict[str, Any]) -> websockets.WebSocketClientProtocol:
        auth = base64.b64encode(f"{db2_server['user']}:{db2_server['password']}".encode()).decode()
        headers = {"Authorization": f"Basic {auth}"}
        
        ssl_context = None
        if db2_server.get('ca'):
            ssl_context = ssl.create_default_context(cafile=db2_server['ca'])
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        uri = f"wss://{db2_server['host']}:{db2_server.get('port', DEFAULT_PORT)}/db/"
        return await websockets.connect(
            uri,
            extra_headers=headers,
            ssl=ssl_context,
        )

    async def send(self, content: str) -> str:
        if self.is_tracing_channel_data:
            print(content)

        req = json.loads(content)
        await self.websocket.send(content)
        self.status = JobStatus.Busy
        
        response = await self.wait_for_response(req['id'])
        
        self.status = JobStatus.Ready if self.get_running_count() == 0 else JobStatus.Busy
        return response

    async def wait_for_response(self, req_id: str) -> str:
        future = asyncio.Future()
        
        def on_response(response):
            if not future.done():
                future.set_result(response)
            self.response_emitter.remove_listener(req_id, on_response)

        self.response_emitter.on(req_id, on_response)
        return await future

    def get_status(self) -> JobStatus:
        return self.status

    def get_running_count(self) -> int:
        return len(self.response_emitter.listeners())

    async def connect(self, db2_server: Dict[str, Any]) -> Dict[str, Any]:
        self.status = JobStatus.Connecting
        self.websocket = await self.get_channel(db2_server)

        props = ";".join(f"{k}={','.join(v) if isinstance(v, list) else v}" for k, v in self.options.items())
        
        connection_object = {
            "id": self.get_new_unique_id(),
            "type": "connect",
            "technique": "tcp",
            "application": "Python client",
            "props": props if props else None
        }

        result = await self.send(json.dumps(connection_object))
        connect_result = json.loads(result)

        if connect_result.get('success') is True:
            self.status = JobStatus.Ready
        else:
            await self.dispose()
            self.status = JobStatus.NotStarted
            raise Exception(connect_result.get('error') or "Failed to connect to server.")

        self.id = connect_result.get('job')
        self.is_tracing_channel_data = False

        return connect_result

    async def dispose(self):
        if self.websocket:
            await self.websocket.close()
        self.websocket = None
        self.status = JobStatus.NotStarted
        self.response_emitter.remove_all_listeners()

    async def message_handler(self):
        try:
            async for message in self.websocket:
                if self.is_tracing_channel_data:
                    print(message)
                
                try:
                    response = json.loads(message)
                    self.response_emitter.emit(response['id'], message)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                except Exception as e:
                    print(f"Error: {e}")
        except websockets.exceptions.ConnectionClosed:
            await self.dispose()

    async def do_connect(self, db2_server: Dict[str, Any]):
        await self.connect(db2_server)
        await self.message_handler()

# Example usage:
async def main():
    db2_server = {
        "host": "your_host",
        "port": 8080,
        "user": "your_username",
        "password": "your_password",
        "ca": "path_to_ca_file",  # Optional
    }
    
    sql_job = SQLJob({"currentSchema": "MYSCHEMA"})
    try:
        await sql_job.run(db2_server)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())