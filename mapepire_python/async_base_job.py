import asyncio
import dataclasses
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union

import websockets
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
        # Correlate responses to in-flight requests by id. Using a plain dict of
        # futures (instead of an event emitter) gives O(1) routing, an exact
        # in-flight count via len(), and lets the count increment synchronously
        # at send time — which is what allows the pool to spread concurrent
        # queries across connections (see Pool.get_job).
        self._pending: Dict[str, "asyncio.Future[Dict[str, Any]]"] = {}
        self._message_task: Optional["asyncio.Task[None]"] = None
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
        logger.debug("sending data: %s", content)
        req = json.loads(content)
        if self.socket is None:
            raise RuntimeError("Socket is not connected")
        req_id = req["id"]
        # Register the pending future *before* awaiting the socket so the
        # in-flight count (and Busy status) is reflected synchronously, before
        # this coroutine yields. Without this, concurrent acquirers would all
        # see the job as idle and pile onto it.
        future: "asyncio.Future[Dict[str, Any]]" = asyncio.get_running_loop().create_future()
        self._pending[req_id] = future
        self.status = JobStatus.Busy
        try:
            await self.socket.send(content)
            logger.debug("waiting for response ...")
            return await future
        finally:
            self._pending.pop(req_id, None)
            self.status = JobStatus.Ready if not self._pending else JobStatus.Busy

    def get_status(self) -> JobStatus:
        return self.status

    def get_running_count(self) -> int:
        count = len(self._pending)
        logger.debug("--- running count %s: %d, status: %s", self.unique_id, count, self.get_status())
        return count

    async def connect(  # type: ignore[override]
        self, db2_server: Optional[Union[DaemonServer, Dict[str, Any], Path]] = None, **kwargs
    ) -> Any:
        db2_server = self._parse_connection_input(db2_server, **kwargs)

        logger.info("Connecting to %s:%s", db2_server.host, db2_server.port)
        self.socket = await self.get_channel(db2_server)
        self._message_task = asyncio.create_task(self.message_handler())

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
            self.id = result.job
            logger.info("Connection established: job_id=%s", self.id)
        else:
            self.status = JobStatus.NotStarted
            logger.error("Connection failed: %s", result.error or "unknown error")
            await self.close()
            raise Exception(result.error or "Failed to connect to server")

        self._is_tracing_channel_data = False

        return result

    async def dispose(self):
        # Cancel the reader task unless dispose() is being called from within it
        # (the server-close path), which would otherwise self-cancel mid-cleanup.
        task = self._message_task
        self._message_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
        if self.socket is not None:
            await self.socket.close()
        self.socket = None
        self.status = JobStatus.NotStarted
        # Fail any still-in-flight requests so their awaiters don't hang forever.
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()

    async def message_handler(self):
        try:
            if self.socket is None:
                raise RuntimeError("Socket is not connected")
            async for message in self.socket:
                logger.debug("Received raw message: %s", message)
                try:
                    response = json.loads(message)
                except json.JSONDecodeError as e:
                    logger.warning("Discarding malformed message: %s", e)
                    continue
                req_id = response.get("id")
                future = self._pending.get(req_id) if req_id else None
                if future is not None and not future.done():
                    future.set_result(response)
                else:
                    logger.debug("No pending request for response id: %s", req_id)
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

    async def close(self) -> None: # type: ignore[override]
        logger.info("Closing job %s", self.id)
        await self.dispose()
        self.status = JobStatus.Ended
        
