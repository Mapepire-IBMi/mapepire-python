"""Connection failures raise PEP 249 ``OperationalError`` with the original error chained."""

import asyncio
import json
import ssl
from unittest.mock import AsyncMock, patch

import pytest
from pep249 import OperationalError
from websockets.exceptions import ConnectionClosedError, InvalidHandshake, InvalidURI

from mapepire_python.client.async_sql_job import AsyncSQLJob
from mapepire_python.client.sql_job import SQLJob
from mapepire_python.websocket import handle_ws_errors

WS_ERRORS = [
    TimeoutError("timed out"),
    ConnectionRefusedError(61, "Connection refused"),
    ssl.SSLCertVerificationError("certificate verify failed"),
    InvalidURI("ws://", "bad uri"),
    InvalidHandshake("handshake failed"),
    ConnectionClosedError(None, None),
]
REJECTED = {"id": "id1", "success": False, "error": "Password is incorrect."}


@pytest.mark.parametrize("error", WS_ERRORS)
def test_sync_ws_error_raises_operational_error(error):
    @handle_ws_errors
    def f():
        raise error

    with pytest.raises(OperationalError) as exc_info:
        f()
    assert exc_info.value.__cause__ is error


@pytest.mark.parametrize("error", WS_ERRORS)
async def test_async_ws_error_raises_operational_error(error):
    @handle_ws_errors
    async def f():
        raise error

    with pytest.raises(OperationalError) as exc_info:
        await f()
    assert exc_info.value.__cause__ is error


def test_sync_sign_on_rejected(mock_creds, mock_socket):
    mock_socket.add_response(json.dumps(REJECTED))
    with patch.object(SQLJob, "_get_channel", return_value=mock_socket):
        with pytest.raises(OperationalError, match="Password is incorrect"):
            SQLJob().connect(mock_creds)


async def test_async_sign_on_rejected(mock_creds):
    job = AsyncSQLJob()
    with (
        patch.object(job, "get_channel", AsyncMock()),
        patch.object(job, "message_handler", AsyncMock()),
        patch.object(job, "send", AsyncMock(return_value=REJECTED)),
    ):
        with pytest.raises(OperationalError, match="Password is incorrect"):
            await job.connect(mock_creds)


def test_sync_dropped_connection(raising_sql_job):
    job, socket = raising_sql_job
    socket.exc = ConnectionClosedError(None, None)
    with pytest.raises(OperationalError):
        job.query_and_run("select 1")


class _ClosedSocket:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def close(self):
        pass


async def test_server_close_fails_pending_requests():
    job = AsyncSQLJob()
    job.socket = _ClosedSocket()  # type: ignore[assignment]
    pending = job._pending["q1"] = asyncio.get_running_loop().create_future()
    await job.message_handler()
    with pytest.raises(OperationalError):
        pending.result()
