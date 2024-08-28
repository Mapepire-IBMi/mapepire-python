import asyncio
import base64
import json
import ssl
from typing import Any, Dict, Optional, Union

import websockets
from pyee.asyncio import AsyncIOEventEmitter

from ..types import DaemonServer, JobStatus, QueryOptions, dict_to_dataclass


class PoolJob:
    unique_id_counter = 0

    def __init__(self, creds: DaemonServer = None, options: Optional[Dict[Any, Any]] = {}):
        self.creds = creds
        self.options = options
        self.socket = None
        self.response_emitter = AsyncIOEventEmitter()
        self.status = JobStatus.NotStarted
        self.trace_file = None
        self.is_tracing_channel_data = False
        self.enable_local_trace = False
        self._unique_id_counter: int = 0
        self.unique_id = self._get_unique_id("sqljob")
        self.id = None
        self.requests = 0

    async def __aenter__(self):
        if self.creds:
            await self.connect(self.creds)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def _get_unique_id(self, prefix: str = "id") -> str:
        self._unique_id_counter += 1
        return f"{prefix}{self._unique_id_counter}"

    def get_unique_id(self):
        return self.unique_id

    def enable_local_trace_data(self):
        self.enable_local_trace = True

    def enable_local_channel_trace(self):
        self.is_tracing_channel_data = True

    def _local_log(self, level: bool, message: str) -> None:
        if level:
            print(message, flush=True)

    async def get_channel(self, db2_server: DaemonServer) -> websockets.WebSocketClientProtocol:
        uri = f"wss://{db2_server.host}:{db2_server.port}/db/"
        headers = {
            "Authorization": "Basic "
            + base64.b64encode(f"{db2_server.user}:{db2_server.password}".encode()).decode("ascii")
        }

        ssl_contest = ssl.create_default_context(cafile=db2_server.ca)
        ssl_contest.check_hostname = False
        ssl_contest.verify_mode = ssl.CERT_NONE

        try:
            socket = await websockets.connect(
                uri=uri, extra_headers=headers, ssl=ssl_contest, ping_timeout=None
            )
        except Exception as e:
            raise e

        return socket

    async def send(self, content: str) -> str:
        self._local_log(self.enable_local_trace, f"sending data: {content}")

        req = json.loads(content)
        await self.socket.send(content)
        self.status = JobStatus.Busy
        self._local_log(self.enable_local_trace, "wating for response ...")
        response = await self.wait_for_response(req["id"])
        self._local_log(self.enable_local_trace, f"recieved response: {response}")
        self.status = JobStatus.Ready if self.get_running_count() == 0 else JobStatus.Busy
        return response

    async def wait_for_response(self, req_id: str) -> str:
        future = asyncio.Future()

        def on_response(response):
            self._local_log(
                self.enable_local_trace, f"Received response for req_id: {req_id} - {response}"
            )
            if not future.done():
                future.set_result(response)
            self.response_emitter.remove_listener(req_id, on_response)

        try:
            self.response_emitter.on(req_id, on_response)
            self._local_log(self.enable_local_trace, f"Listener registered for req_id: {req_id}")
            return await future
        except Exception as e:
            self.response_emitter.remove_listener(req_id, on_response)
            raise e

    def get_status(self) -> JobStatus:
        return self.status

    def get_running_count(self) -> int:
        self._local_log(
            self.enable_local_trace,
            f"--- running count {self.unique_id}: {len(self.response_emitter.event_names())}, status: {self.get_status()}",
        )
        return len(self.response_emitter.event_names())

    async def connect(self, db2_server: Union[DaemonServer, Dict[str, Any]]) -> Dict[str, Any]:
        if isinstance(db2_server, dict):
            db2_server = dict_to_dataclass(db2_server, DaemonServer)

        # create socket connection
        self.socket = await self.get_channel(db2_server)

        # start async task for handle websocket messages
        asyncio.create_task(self.message_handler())

        # format optional args
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

        result = await self.send(json.dumps(connection_props))

        if result.get("success", False):
            self.status = JobStatus.Ready
        else:
            self.status = JobStatus.NotStarted
            print(result)
            raise Exception(result.get("error", "Failed to connect to server"))

        self.id = result["job"]
        self._is_tracing_channel_data = False

        return result

    async def dispose(self):
        if self.socket:
            await self.socket.close()
        self.socket = None
        self.status = JobStatus.NotStarted
        self.response_emitter.remove_all_listeners()

    async def message_handler(self):
        try:
            async for message in self.socket:
                self._local_log(self.enable_local_trace, f"Received raw message: {message}")

                try:
                    response = json.loads(message)
                    req_id = response.get("id")
                    if req_id:
                        self._local_log(
                            self.enable_local_trace, f"Emitting response for req_id: {req_id}"
                        )
                        self.response_emitter.emit(req_id, response)
                    else:
                        self._local_log(
                            self.enable_local_trace, f"No req_id found in response: {response}"
                        )
                except json.JSONDecodeError as e:
                    raise ValueError(f"Error decoding JSON: {e}")
                except Exception as e:
                    raise RuntimeError(f"Error: {e}")
        except websockets.exceptions.ConnectionClosed:
            await self.dispose()

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
        from .pool_query import PoolQuery

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

        return PoolQuery(job=self, query=sql, opts=query_options)

    async def query_and_run(
        self, sql: str, opts: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Dict[str, Any]:
        query = self.query(sql, opts)
        return await query.run(**kwargs)

    async def close(self):
        self.status = JobStatus.Ended
        await self.socket.close()
