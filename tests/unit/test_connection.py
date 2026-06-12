"""Unit tests for core/connection.py.

Covers connection state management, cursor creation and closed-connection
detection.  ``SQLJob`` is patched so ``Connection.__init__`` performs no
network I/O — we only verify the Connection's own behaviour.
"""
from unittest.mock import MagicMock, patch

import pytest
from pep249 import ProgrammingError

from mapepire_python.core.connection import Connection
from mapepire_python.core.cursor import Cursor


@pytest.fixture
def connection(mock_creds):
    """A Connection whose underlying SQLJob is a MagicMock (no network)."""
    with patch("mapepire_python.core.connection.SQLJob") as job_cls:
        job = MagicMock()
        job.id = "TEST/QUSER/JOB001"
        job_cls.return_value = job
        conn = Connection(mock_creds)
        conn._mock_job = job  # expose the mock for assertions
        yield conn


# ---------------------------------------------------------------------------
# Construction / state
# ---------------------------------------------------------------------------

class TestConstruction:
    def test_starts_open(self, connection):
        assert connection._closed is False

    def test_connect_called_on_init(self, connection):
        connection._mock_job.connect.assert_called_once()

    def test_job_is_created(self, connection):
        assert connection.job is connection._mock_job


# ---------------------------------------------------------------------------
# cursor()
# ---------------------------------------------------------------------------

class TestCursorCreation:
    def test_cursor_returns_cursor_instance(self, connection):
        assert isinstance(connection.cursor(), Cursor)

    def test_cursor_shares_connection_job(self, connection):
        assert connection.cursor().job is connection.job

    def test_each_cursor_is_distinct(self, connection):
        assert connection.cursor() is not connection.cursor()

    def test_cursor_back_references_connection(self, connection):
        # Cursor.connection is a weakref.proxy, so compare via the shared job.
        assert connection.cursor().connection.job is connection.job


# ---------------------------------------------------------------------------
# close() / closed detection
# ---------------------------------------------------------------------------

class TestClose:
    def test_close_sets_closed_flag(self, connection):
        connection.close()
        assert connection._closed is True

    def test_close_closes_underlying_job(self, connection):
        connection.close()
        connection._mock_job.close.assert_called_once()

    def test_close_is_idempotent(self, connection):
        connection.close()
        connection.close()
        connection._mock_job.close.assert_called_once()

    def test_cursor_on_closed_connection_raises(self, connection):
        connection.close()
        with pytest.raises(ProgrammingError, match="closed connection"):
            connection.cursor()


# ---------------------------------------------------------------------------
# commit / rollback delegate to the job
# ---------------------------------------------------------------------------

class TestTransactions:
    def test_commit_runs_commit_statement(self, connection):
        connection.commit()
        connection._mock_job.query_and_run.assert_called_once_with("COMMIT")

    def test_rollback_runs_rollback_statement(self, connection):
        connection.rollback()
        connection._mock_job.query_and_run.assert_called_once_with("ROLLBACK")
