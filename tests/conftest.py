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