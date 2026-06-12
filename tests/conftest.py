"""Shared fixtures for mapepire-python tests.

Unit test fixtures live here so they are available to tests/unit/ without
requiring a live IBM i server connection.  Integration tests continue to use
tests/test_setup.py for credentials.
"""
import json
from typing import List, Optional

import pytest

from mapepire_python import DaemonServer, JobStatus, SQLJob


class MockSocket:

    def __init__(self, responses: Optional[List[str]] = None) -> None:
        self._response_queue: List[str] = list(responses or [])
        self.sent: List[str] = []
        self.closed: bool = False

    def send(self, data: str) -> None:
        self.sent.append(data)

    def recv(self) -> str:
        if not self._response_queue:
            raise RuntimeError("MockSocket: no more responses queued")
        return self._response_queue.pop(0)

    def close(self) -> None:
        self.closed = True

    def add_response(self, response: str) -> None:
        self._response_queue.append(response)

    def __len__(self) -> int:
        return len(self._response_queue)


class AsyncMockSocket:
    """Async counterpart to ``MockSocket`` for the asyncio WebSocket path.

    ``send``/``recv``/``close`` are coroutines, matching the ``websockets``
    async connection interface so it can be injected into async jobs.
    """

    def __init__(self, responses: Optional[List[str]] = None) -> None:
        self._response_queue: List[str] = list(responses or [])
        self.sent: List[str] = []
        self.closed: bool = False

    async def send(self, data: str) -> None:
        self.sent.append(data)

    async def recv(self) -> str:
        if not self._response_queue:
            raise RuntimeError("AsyncMockSocket: no more responses queued")
        return self._response_queue.pop(0)

    async def close(self) -> None:
        self.closed = True

    def add_response(self, response: str) -> None:
        self._response_queue.append(response)

    def __len__(self) -> int:
        return len(self._response_queue)


class RaisingSocket:
    """Socket whose I/O raises, simulating a transport-level failure.

    ``recv``/``send`` raise ``exc`` (default ``ConnectionError``) so tests can
    exercise error-recovery paths around a dropped or refused connection.
    """

    def __init__(self, exc: Optional[Exception] = None) -> None:
        self.exc = exc or ConnectionError("connection lost")
        self.sent: List[str] = []
        self.closed: bool = False

    def send(self, data: str) -> None:
        raise self.exc

    def recv(self) -> str:
        raise self.exc

    def close(self) -> None:
        self.closed = True



@pytest.fixture
def mock_creds() -> DaemonServer:
    """DaemonServer with fake credentials – no network calls are made."""
    return DaemonServer(
        host="test.example.com",
        user="testuser",
        password="testpass",
        port=8076,
        ignoreUnauthorized=True,
    )


@pytest.fixture
def mock_socket() -> MockSocket:
    """Bare MockSocket with no pre-loaded responses."""
    return MockSocket()


@pytest.fixture
def mock_sql_job():
    """Pre-connected SQLJob backed by a MockSocket.

    Returns a ``(job, socket)`` tuple.  The job is in ``JobStatus.Ready``
    state with its ``_socket`` already injected so tests can enqueue
    responses via ``socket.add_response(...)``.
    """
    job = SQLJob()
    socket = MockSocket()
    job._socket = socket  # type: ignore[assignment]
    job._status = JobStatus.Ready
    job.id = "TEST/QUSER/JOB001"
    return job, socket




@pytest.fixture
def make_connect_result():
    """Return a factory for connection-result JSON strings."""
    def _make(
        job_id: str = "TEST/QUSER/JOB001",
        success: bool = True,
        error: Optional[str] = None,
    ) -> str:
        return json.dumps({
            "id": "connect1",
            "success": success,
            "sql_rc": 0,
            "sql_state": "",
            "job": job_id,
            "error": error,
            "execution_time": None,
        })
    return _make


@pytest.fixture
def make_query_result():
    """Return a factory for QueryResult JSON strings."""
    def _make(
        data: Optional[list] = None,
        is_done: bool = True,
        has_results: bool = False,
        success: bool = True,
        id: str = "query1",
        error: Optional[str] = None,
        update_count: int = 0,
        sql_rc: int = 0,
        sql_state: str = "",
        metadata: Optional[dict] = None,
    ) -> str:
        return json.dumps({
            "id": id,
            "success": success,
            "sql_rc": sql_rc,
            "sql_state": sql_state,
            "is_done": is_done,
            "has_results": has_results,
            "update_count": update_count,
            "data": data if data is not None else [],
            "metadata": metadata,
            "error": error,
            "execution_time": None,
        })
    return _make


@pytest.fixture
def make_sql_more_response():
    """Return a factory for SqlMoreResponse JSON strings."""
    def _make(
        data: Optional[list] = None,
        is_done: bool = True,
        success: bool = True,
        id: str = "sqlmore1",
        error: Optional[str] = None,
    ) -> str:
        return json.dumps({
            "id": id,
            "success": success,
            "sql_rc": 0,
            "sql_state": "",
            "is_done": is_done,
            "data": data if data is not None else [],
            "error": error,
            "execution_time": None,
        })
    return _make


@pytest.fixture
def make_close_response():
    """Return a factory for SqlCloseResponse JSON strings."""
    def _make(id: str = "sqlclose1", success: bool = True) -> str:
        return json.dumps({
            "id": id,
            "success": success,
            "sql_rc": 0,
            "sql_state": "",
            "error": None,
            "execution_time": None,
        })
    return _make


# ---------------------------------------------------------------------------
# Error-condition fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def make_error_query_result(make_query_result):
    """Return a factory for *failed* QueryResult JSON strings.

    Defaults mimic a real SQL error: ``success=False`` with a populated
    ``error`` message and non-zero ``sql_rc``/``sql_state``.
    """
    def _make(
        error: str = "SQL error occurred",
        sql_rc: int = -104,
        sql_state: str = "42000",
        id: str = "query_err",
    ) -> str:
        return make_query_result(
            success=False,
            is_done=False,
            error=error,
            sql_rc=sql_rc,
            sql_state=sql_state,
            id=id,
        )
    return _make


@pytest.fixture
def error_socket(make_error_query_result) -> MockSocket:
    """MockSocket pre-loaded with a single failed-query response."""
    return MockSocket([make_error_query_result()])


@pytest.fixture
def error_sql_job(mock_sql_job, make_error_query_result):
    """``(job, socket)`` like ``mock_sql_job`` but queued with a failed response.

    The next ``recv`` the job performs returns a ``success=False`` QueryResult,
    so query execution against this job raises.
    """
    job, socket = mock_sql_job
    socket.add_response(make_error_query_result())
    return job, socket


@pytest.fixture
def raising_socket() -> RaisingSocket:
    """Socket whose ``recv``/``send`` raise, simulating a dropped connection."""
    return RaisingSocket()


@pytest.fixture
def raising_sql_job(mock_sql_job):
    """``(job, socket)`` whose socket raises on I/O, simulating transport loss."""
    job, _ = mock_sql_job
    socket = RaisingSocket()
    job._socket = socket  # type: ignore[assignment]
    return job, socket


@pytest.fixture
def async_mock_socket() -> AsyncMockSocket:
    """Bare AsyncMockSocket with no pre-loaded responses."""
    return AsyncMockSocket()