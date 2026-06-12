"""Unit tests for Cursor lifecycle, execute() wiring and closed detection.

Complements tests/test_cursor_pep249.py (which covers description/fetch/rowcount)
by focusing on:
  - execute() state wiring (has_results, query queue) for SELECT vs DML
  - close() behaviour and idempotency
  - closed-cursor detection via raise_if_closed (own close, parent close, GC'd parent)

Uses MockSocket-backed jobs from tests/conftest.py — no live server.
"""
import gc

import pytest
from pep249 import DatabaseError, ProgrammingError

from mapepire_python import Cursor


class _FakeConn:
    """Minimal connection stub — just enough for Cursor._closed checks."""
    _closed = False


def _make_cursor(mock_sql_job):
    """Return (Cursor, MockSocket, conn) wired to the mock job.

    The conn is returned so the caller keeps a strong reference — Cursor holds
    only a weakref and a dead weakref forces _closed=True.
    """
    job, socket = mock_sql_job
    conn = _FakeConn()
    cursor = Cursor(conn, job)
    return cursor, socket, conn


# ---------------------------------------------------------------------------
# execute() state wiring
# ---------------------------------------------------------------------------

class TestExecuteWiring:
    def test_select_sets_has_results_and_queues_query(self, mock_sql_job, make_query_result):
        cursor, socket, _conn = _make_cursor(mock_sql_job)
        socket.add_response(make_query_result(is_done=True, has_results=True))
        cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        assert cursor.has_results is True
        assert cursor.query is not None
        assert len(cursor.query_q) == 1

    def test_dml_clears_has_results(self, mock_sql_job, make_query_result):
        cursor, socket, _conn = _make_cursor(mock_sql_job)
        socket.add_response(make_query_result(is_done=True, has_results=False, update_count=3))
        cursor.execute("UPDATE T SET C = 1")
        assert cursor.has_results is False

    def test_dml_update_count_sets_rowcount(self, mock_sql_job, make_query_result):
        cursor, socket, _conn = _make_cursor(mock_sql_job)
        socket.add_response(make_query_result(is_done=True, has_results=False, update_count=7))
        cursor.execute("DELETE FROM T")
        assert cursor.rowcount == 7

    def test_execute_returns_cursor_for_chaining(self, mock_sql_job, make_query_result):
        cursor, socket, _conn = _make_cursor(mock_sql_job)
        socket.add_response(make_query_result(is_done=True, has_results=True))
        assert cursor.execute("SELECT 1") is cursor

    def test_executemany_delegates_to_execute(self, mock_sql_job, make_query_result):
        cursor, socket, _conn = _make_cursor(mock_sql_job)
        socket.add_response(make_query_result(is_done=True, has_results=False, update_count=1))
        result = cursor.executemany("INSERT INTO T VALUES (?)", [[1], [2]])
        assert result is cursor

    def test_failed_execute_raises_database_error(self, mock_sql_job, make_query_result):
        # convert_runtime_errors maps the RuntimeError onto a PEP 249 error.
        cursor, socket, _conn = _make_cursor(mock_sql_job)
        socket.add_response(make_query_result(success=False, is_done=False, error="bad sql"))
        with pytest.raises(DatabaseError):
            cursor.execute("SELECT bogus")


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------

class TestClose:
    def test_close_after_execute_marks_closed_and_clears_queue(
        self, mock_sql_job, make_query_result
    ):
        cursor, socket, _conn = _make_cursor(mock_sql_job)
        socket.add_response(make_query_result(is_done=True, has_results=True))
        cursor.execute("SELECT 1")
        cursor.close()
        assert cursor._closed is True
        assert len(cursor.query_q) == 0

    def test_close_is_idempotent(self, mock_sql_job, make_query_result):
        cursor, socket, _conn = _make_cursor(mock_sql_job)
        socket.add_response(make_query_result(is_done=True, has_results=True))
        cursor.execute("SELECT 1")
        cursor.close()
        cursor.close()  # second call returns immediately, no error
        assert cursor._closed is True


# ---------------------------------------------------------------------------
# closed-cursor detection (raise_if_closed)
# ---------------------------------------------------------------------------

class TestClosedDetection:
    def test_execute_on_closed_cursor_raises(self, mock_sql_job):
        cursor, _socket, _conn = _make_cursor(mock_sql_job)
        cursor._closed = True
        with pytest.raises(ProgrammingError, match="closed connection"):
            cursor.execute("SELECT 1")

    def test_fetchone_on_closed_cursor_raises(self, mock_sql_job):
        cursor, _socket, _conn = _make_cursor(mock_sql_job)
        cursor._closed = True
        with pytest.raises(ProgrammingError):
            cursor.fetchone()

    def test_fetchall_on_closed_cursor_raises(self, mock_sql_job):
        cursor, _socket, _conn = _make_cursor(mock_sql_job)
        cursor._closed = True
        with pytest.raises(ProgrammingError):
            cursor.fetchall()

    def test_cursor_is_closed_when_parent_connection_closed(self, mock_sql_job):
        cursor, _socket, conn = _make_cursor(mock_sql_job)
        conn._closed = True
        assert cursor._closed is True

    def test_cursor_is_closed_when_parent_connection_gc_d(self, mock_sql_job):
        job, _socket = mock_sql_job
        conn = _FakeConn()
        cursor = Cursor(conn, job)
        del conn
        gc.collect()
        # weakref.proxy is now dead -> _closed property returns True
        assert cursor._closed is True
