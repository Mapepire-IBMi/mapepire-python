import asyncio
import dataclasses
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union

import websockets
from pyee.asyncio import AsyncIOEventEmitter
from websockets.asyncio.client import ClientConnection

from .base_job import BaseJob
from .data_types import (
    ConnectionResult,
    ConnectRequest,
    DaemonServer,
    JobStatus,
    QueryOptions,
)

__all__ = ["AsyncBaseJob"]

logger = logging.getLogger(__name__)


class AsyncBaseJob(BaseJob):
    """Shared async WebSocket infrastructure for PoolJob and AsyncSQLJob.

    Provides the full native async lifecycle: WebSocket connection,
    event-driven message routing, request/response correlation, and
    query execution via PoolQuery. Subclasses add only what is unique
    to their use case.
    """

    def __init__(
        self,
        creds: Optional[Union[DaemonServer, Dict[str, Any], Path]] = None,
        options: Optional[Dict[Any, Any]] = None,
        **kwargs,
    ) -> None:
        super().__init__(creds, options or {}, **kwargs)
        self.socket: Optional[ClientConnection] = None
        self.response_emitter = AsyncIOEventEmitter()
        self.status = JobStatus.NotStarted
        self._unique_id_counter: int = 0
        self.unique_id = self._get_unique_id("sqljob")
        self.id: Optional[str] = None

    async def __aenter__(self):
        if self.creds:
            await self.connect(self.creds, **self.kwargs)
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.close()

    def _get_unique_id(self, prefix: str = "id") -> str:
        self._unique_id_counter += 1
        return f"{prefix}{self._unique_id_counter}"

    def get_unique_id(self) -> str:
        return self.unique_id

    async def get_channel(self, db2_server: DaemonServer) -> ClientConnection:
        from mapepire_python.pool.async_websocket_client import AsyncWebSocketConnection

        socket = AsyncWebSocketConnection(db2_server)
        return await socket.connect()

    async def send(self, content: str) -> Dict[str, Any]:
        logger.debug(f"sending data: {content}")
        req = json.loads(content)
        if self.socket is None:
            raise RuntimeError("Socket is not connected")
        try:
            await self.socket.send(content)
            self.status = JobStatus.Busy
            logger.debug("waiting for response ...")
            response = await self.wait_for_response(req["id"])
            self.status = JobStatus.Ready if self.get_running_count() == 0 else JobStatus.Busy
            return response
        except Exception as e:
            raise e

    async def wait_for_response(self, req_id: str) -> Dict[str, Any]:
        future: asyncio.Future = asyncio.Future()

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

    async def connect(  # type: ignore[override]
        self, db2_server: Union[DaemonServer, Dict[str, Any], Path], **kwargs
    ) -> Any:
        db2_server = self._parse_connection_input(db2_server, **kwargs)

        self.socket = await self.get_channel(db2_server)
        asyncio.create_task(self.message_handler())

        props = ";".join(
            [
                f'{prop}={",".join(self.options[prop]) if isinstance(self.options[prop], list) else self.options[prop]}'
                for prop in self.options
            ]
        )

        connect_req = ConnectRequest(
            id=self._get_unique_id(),
            props=props if len(props) > 0 else "",
        )

        result = ConnectionResult.from_dict(  # type: ignore[attr-defined]
            await self.send(json.dumps(dataclasses.asdict(connect_req)))
        )

        if result.success:
            self.status = JobStatus.Ready
        else:
            self.status = JobStatus.NotStarted
            await self.close()
            raise Exception(result.error or "Failed to connect to server")

        self.id = result.job
        self._is_tracing_channel_data = False

        return result

    async def dispose(self):
        if self.socket is not None:
            await self.socket.close()
        self.socket = None
        self.status = JobStatus.NotStarted
        self.response_emitter.remove_all_listeners()

    async def message_handler(self):
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
        from mapepire_python.pool.pool_query import PoolQuery

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

    async def query_and_run(  # type: ignore[override]
        self, sql: str, opts: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Any:
        rows_to_fetch: Optional[int] = kwargs.get("rows_to_fetch")
        try:
            async with self.query(sql, opts) as query:
                return await query.run(rows_to_fetch=rows_to_fetch)
        except Exception as e:
            raise RuntimeError(f"Failed to run query: {e}") from e

    async def close(self) -> None:  # type: ignore[override]
        self.status = JobStatus.Ended
        if self.socket:
            await self.socket.close()
