import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union

import websockets
from pyee.asyncio import AsyncIOEventEmitter
from websockets.asyncio.client import ClientConnection

from mapepire_python.pool.async_websocket_client import AsyncWebSocketConnection

from ..base_job import BaseJob
from ..data_types import DaemonServer, JobStatus, QueryOptions

__all__ = ["PoolJob"]

logger = logging.getLogger("websockets.client")


class PoolJob(BaseJob):
    unique_id_counter = 0

    def __init__(
        self,
        creds: Optional[Union[DaemonServer, Dict[str, Any], Path]] = None,
        options: Optional[Dict[Any, Any]] = None,
        **kwargs,
    ) -> None:
        super().__init__(creds, options or {}, **kwargs)
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
            await self.connect(self.creds, **self.kwargs)
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.close()

    def _get_unique_id(self, prefix: str = "id") -> str:
        """returns a unique id for the job object with the given prefix

        Args:
            prefix (str, optional): _description_. Defaults to "id".

        Returns:
            str: unique id
        """
        self._unique_id_counter += 1
        return f"{prefix}{self._unique_id_counter}"

    def get_unique_id(self):
        return self.unique_id

    def enable_local_trace_data(self):
        self.enable_local_trace = True

    def enable_local_channel_trace(self):
        self.is_tracing_channel_data = True

    async def get_channel(self, db2_server: DaemonServer) -> ClientConnection:
        """returns a websocket connection to the mapepire server

        Args:
            db2_server (DaemonServer): server credentials

        Raises:
            TimeoutError: Failed to connect to server
            e: Exception

        Returns:
            websockets.WebSocketClientProtocol: websocket connection
        """
        socket = AsyncWebSocketConnection(db2_server)
        return await socket.connect()

    async def send(self, content: str) -> Dict[Any, Any]:
        """sends content to the mapepire server

        Args:
            content (str): JSON content to be sent

        Returns:
            str: response from the server
        """
        logger.debug(f"sending data: {content}")

        req = json.loads(content)
        if self.socket is None:
            raise RuntimeError("Socket is not connected")
        try:
            await self.socket.send(content)
            self.status = JobStatus.Busy
            logger.debug("waiting for response ...")
            response = await self.wait_for_response(req["id"])
            # logger.debug(f"received response: {response}")
            self.status = JobStatus.Ready if self.get_running_count() == 0 else JobStatus.Busy
            return response  # type: ignore
        except Exception as e:
            raise e

    async def wait_for_response(self, req_id: str) -> str:
        """when a request is sent to the server, this method waits for the response

        Args:
            req_id (str): request id

        Raises:
            e: Exception

        Returns:
            str: response from the server
        """
        future = asyncio.Future()

        def on_response(response):
            logger.debug(f"Received response for req_id: {req_id} - {response}")
            if not future.done():
                future.set_result(response)
            self.response_emitter.remove_listener(req_id, on_response)

        try:
            self.response_emitter.on(req_id, on_response)
            logger.debug(f"Listener registered for req_id: {req_id}")
            return await future
        except Exception as e:
            self.response_emitter.remove_listener(req_id, on_response)
            raise e

    def get_status(self) -> JobStatus:
        return self.status

    def get_running_count(self) -> int:
        logger.debug(
            f"--- running count {self.unique_id}: {len(self.response_emitter.event_names())}, status: {self.get_status()}"
        )
        return len(self.response_emitter.event_names())

    async def connect(  # type: ignore
        self, db2_server: Union[DaemonServer, Dict[str, Any], Path], **kwargs
    ) -> Any:
        """create connection to the mapepire server

        Args:
            db2_server (Union[DaemonServer, Dict[str, Any]]): server credentials

        Raises:
            Exception: Failed to connect to server

        Returns:
            Dict[str, Any]: Connection results from the server
        """
        db2_server = self._parse_connection_input(db2_server, **kwargs)

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

        if result.get("success", False):  # type: ignore
            self.status = JobStatus.Ready
        else:
            self.status = JobStatus.NotStarted
            await self.close()
            raise Exception(result.get("error", "Failed to connect to server"))  # type: ignore

        self.id = result["job"]  # type: ignore
        self._is_tracing_channel_data = False

        return result

    async def dispose(self):
        if self.socket:
            if self.socket is not None:
                await self.socket.close()
        self.socket = None
        self.status = JobStatus.NotStarted
        self.response_emitter.remove_all_listeners()

    async def message_handler(self):
        """handle incoming messages from the server

        Raises:
            ValueError: Error decoding JSON
            RuntimeError: Error occured while processing message
        """
        try:
            if self.socket is None:
                raise RuntimeError("Socket is not connected")
            async for message in self.socket:
                logger.debug(f"Received raw message: {message}")

                try:
                    response = json.loads(message)
                    req_id = response.get("id")
                    if req_id:
                        logger.debug(f"Emitting response for req_id: {req_id}")
                        self.response_emitter.emit(req_id, response)
                    else:
                        logger.debug(f"No req_id found in response: {response}")
                except json.JSONDecodeError as e:
                    raise ValueError(f"Error decoding JSON: {e}")
                except Exception as e:
                    raise RuntimeError(f"Error: {e}")
        except websockets.exceptions.ConnectionClosedError:
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

    async def query_and_run(  # type: ignore
        self, sql: str, opts: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Dict[str, Any]:
        """Create a PoolQuery object using provided SQL and options, then run the query.

        Args:
            sql (str): query string
            opts (Optional[Dict[str, Any]], optional): Query Options, Defaults to None.

        Raises:
            RuntimeError: Failed to run query

        Returns:
            Dict[str, Any]: Results of the query
        """
        try:
            async with self.query(sql, opts) as query:
                return await query.run(**kwargs)
        except Exception as e:
            raise RuntimeError(f"Failed to run query: {e}")

    async def close(self) -> None:  # type: ignore
        self.status = JobStatus.Ended
        if self.socket:
            await self.socket.close()
