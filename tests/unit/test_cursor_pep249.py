"""Unit tests for PEP 249 cursor compliance (no live server required).

Covers:
  - cursor.description: 7-tuple structure, field mapping, pre/post execute state
  - fetchone() / fetchall() / fetchmany(): return tuples not dicts, correct values
  - fetchone() returns None when exhausted
  - rowcount: -1 for SELECT, actual count for DML
"""
import json

import pytest

from mapepire_python import Cursor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COLUMNS = [
    {"name": "EMPNO",    "label": "EMPNO",    "type": "CHAR",    "display_size": 6,
     "precision": None, "scale": None, "nullable": False},
    {"name": "FIRSTNME", "label": "FIRSTNME", "type": "VARCHAR", "display_size": 12,
     "precision": None, "scale": None, "nullable": True},
    {"name": "SALARY",   "label": "SALARY",   "type": "DECIMAL", "display_size": 10,
     "precision": 9,    "scale": 2,    "nullable": True},
]

_METADATA = {"column_count": 3, "job": "TEST/QUSER/JOB001", "columns": _COLUMNS}

_ROWS = [
    {"EMPNO": "000010", "FIRSTNME": "JOHN",  "SALARY": 52750.00},
    {"EMPNO": "000020", "FIRSTNME": "ALICE", "SALARY": 41250.00},
    {"EMPNO": "000030", "FIRSTNME": "BOB",   "SALARY": 38500.00},
]


class _FakeConn:
    """Minimal connection stub — just enough for Cursor._closed checks."""
    _closed = False


def _make_cursor(mock_sql_job):
    """Return a (Cursor, MockSocket) wired to the mock job.

    The _FakeConn instance is attached to the cursor to prevent it from being
    GC'd — Cursor holds only a weakref, and a dead weakref makes _closed=True.
    """
    job, socket = mock_sql_job
    conn = _FakeConn()
    cursor = Cursor(conn, job)
    cursor._test_conn_ref = conn  # keep strong reference alive
    return cursor, socket


def _queue_execute(socket, rows=None, is_done=False, update_count=-1, metadata=None):
    """Queue the prepare_sql_execute response for a SELECT, optionally carrying rows."""
    socket.add_response(json.dumps({
        "id": "q1",
        "success": True,
        "sql_rc": 0,
        "sql_state": "",
        "is_done": is_done,
        "has_results": True,
        "update_count": update_count,
        "data": rows if rows is not None else [],
        "metadata": metadata if metadata is not None else _METADATA,
        "error": None,
        "execution_time": None,
    }))


def _queue_page(socket, rows, is_done=True):
    """Queue one sqlmore response."""
    socket.add_response(json.dumps({
        "id": "q1",
        "success": True,
        "sql_rc": 0,
        "sql_state": "",
        "is_done": is_done,
        "data": rows,
        "error": None,
        "execution_time": None,
    }))


def _queue_select(socket, rows=None, update_count=-1, metadata=None, is_done_on_fetch=True):
    """Queue the two socket responses needed for execute() + one fetch call."""
    # 1. prepare_sql_execute response: metadata present, no data rows yet
    _queue_execute(socket, update_count=update_count, metadata=metadata)
    # 2. sqlmore response: actual data rows
    _queue_page(socket, rows if rows is not None else _ROWS, is_done=is_done_on_fetch)


def _queue_close(socket):
    """Queue one sqlclose response, for closing a cursor whose query is still open."""
    socket.add_response(json.dumps({
        "id": "q1", "success": True, "sql_rc": 0, "sql_state": "", "error": None,
    }))


def _sqlmore_requests(socket):
    """The sqlmore requests the cursor has sent, decoded."""
    return [msg for msg in map(json.loads, socket.sent) if msg["type"] == "sqlmore"]


_ROW_TUPLES = [
    ("000010", "JOHN",  52750.00),
    ("000020", "ALICE", 41250.00),
    ("000030", "BOB",   38500.00),
]


def _queue_dml(socket, update_count=3):
    """Queue the single socket response for a DML execute()."""
    socket.add_response(json.dumps({
        "id": "q2",
        "success": True,
        "sql_rc": 0,
        "sql_state": "",
        "is_done": True,
        "has_results": False,
        "update_count": update_count,
        "data": [],
        "metadata": None,
        "error": None,
        "execution_time": None,
    }))


# ---------------------------------------------------------------------------
# cursor.description
# ---------------------------------------------------------------------------

class TestDescription:
    def test_none_before_execute(self, mock_sql_job):
        cursor, _ = _make_cursor(mock_sql_job)
        assert cursor.description is None

    def test_none_after_dml(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_dml(socket)
        cursor.execute("DELETE FROM SAMPLE.EMPLOYEE WHERE EMPNO = '000010'")
        assert cursor.description is None

    def test_returns_sequence_after_select(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT EMPNO, FIRSTNME, SALARY FROM SAMPLE.EMPLOYEE")
        desc = cursor.description
        assert desc is not None
        assert len(desc) == 3

    def test_each_entry_is_seven_tuple(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT EMPNO, FIRSTNME, SALARY FROM SAMPLE.EMPLOYEE")
        for col in cursor.description:
            assert len(col) == 7

    def test_name_mapping(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT EMPNO, FIRSTNME, SALARY FROM SAMPLE.EMPLOYEE")
        desc = cursor.description
        assert desc[0][0] == "EMPNO"
        assert desc[1][0] == "FIRSTNME"
        assert desc[2][0] == "SALARY"

    def test_type_code_mapping(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT EMPNO, FIRSTNME, SALARY FROM SAMPLE.EMPLOYEE")
        desc = cursor.description
        assert desc[0][1] == str    # CHAR
        assert desc[1][1] == str    # VARCHAR
        assert desc[2][1] == float  # DECIMAL

    def test_display_size_mapping(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT EMPNO, FIRSTNME, SALARY FROM SAMPLE.EMPLOYEE")
        desc = cursor.description
        assert desc[0][2] == 6   # EMPNO
        assert desc[1][2] == 12  # FIRSTNME
        assert desc[2][2] == 10  # SALARY

    def test_internal_size_is_always_none(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT EMPNO, FIRSTNME, SALARY FROM SAMPLE.EMPLOYEE")
        for col in cursor.description:
            assert col[3] is None

    def test_precision_scale_nullable_mapping(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT EMPNO, FIRSTNME, SALARY FROM SAMPLE.EMPLOYEE")
        desc = cursor.description
        # SALARY: precision=9, scale=2, nullable=True
        assert desc[2][4] == 9
        assert desc[2][5] == 2
        assert desc[2][6] is True
        # EMPNO: nullable=False
        assert desc[0][6] is False

    def test_unknown_sql_type_falls_back_to_str(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket, metadata={
            "column_count": 1,
            "job": "TEST/QUSER/JOB001",
            "columns": [{"name": "X", "label": "X", "type": "EXOTIC_DBTYPE",
                         "display_size": 10, "precision": None, "scale": None, "nullable": None}],
        })
        cursor.execute("SELECT X FROM T")
        assert cursor.description[0][1] == str


# ---------------------------------------------------------------------------
# fetchone()
# ---------------------------------------------------------------------------

class TestFetchone:
    def test_returns_tuple(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        row = cursor.fetchone()
        assert isinstance(row, tuple)

    def test_values_ordered_by_metadata_columns(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        # column order: EMPNO, FIRSTNME, SALARY
        assert cursor.fetchone() == ("000010", "JOHN", 52750.00)

    def test_returns_none_before_execute(self, mock_sql_job):
        cursor, _ = _make_cursor(mock_sql_job)
        assert cursor.fetchone() is None

    def test_returns_none_when_query_is_done(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket, rows=[_ROWS[0]], is_done_on_fetch=True)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        cursor.fetchone()  # exhausts the single row; query enters RUN_DONE
        assert cursor.fetchone() is None

    def test_terse_list_row_becomes_tuple(self, mock_sql_job):
        """Terse-mode rows arrive as lists; fetchone must still return tuples."""
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket, rows=[["000010", "JOHN", 52750.00]])
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        row = cursor.fetchone()
        assert isinstance(row, tuple)
        assert row == ("000010", "JOHN", 52750.00)


# ---------------------------------------------------------------------------
# fetchall()
# ---------------------------------------------------------------------------

class TestFetchall:
    def test_returns_list_of_tuples(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        rows = cursor.fetchall()
        assert isinstance(rows, list)
        assert len(rows) == 3
        assert all(isinstance(r, tuple) for r in rows)

    def test_row_values(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        rows = cursor.fetchall()
        assert rows[0] == ("000010", "JOHN",  52750.00)
        assert rows[1] == ("000020", "ALICE", 41250.00)
        assert rows[2] == ("000030", "BOB",   38500.00)

    def test_returns_empty_list_before_execute(self, mock_sql_job):
        cursor, _ = _make_cursor(mock_sql_job)
        assert cursor.fetchall() == []


# ---------------------------------------------------------------------------
# fetchmany()
# ---------------------------------------------------------------------------

class TestFetchmany:
    def test_returns_list_of_tuples(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        rows = cursor.fetchmany(2)
        assert isinstance(rows, list)
        assert all(isinstance(r, tuple) for r in rows)

    def test_size_respected_by_server_response(self, mock_sql_job):
        """fetchmany(2) passes size=2 to the server; if server honours it we get 2 rows."""
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket, rows=_ROWS[:2])  # server returns 2
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        rows = cursor.fetchmany(2)
        assert len(rows) == 2

    def test_returns_empty_list_before_execute(self, mock_sql_job):
        cursor, _ = _make_cursor(mock_sql_job)
        assert cursor.fetchmany(5) == []


# ---------------------------------------------------------------------------
# exhaustion: fetch* return empty instead of raising once rows run out
# ---------------------------------------------------------------------------

class TestFetchExhaustion:
    def test_fetchmany_loop_ends_with_empty_list(self, mock_sql_job):
        """The standard `while rows := cur.fetchmany(n)` loop must terminate cleanly."""
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket, rows=_ROWS[:2], is_done_on_fetch=False)
        _queue_page(socket, _ROWS[2:], is_done=True)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        fetched = []
        while rows := cursor.fetchmany(2):
            fetched.extend(rows)
        assert fetched == _ROW_TUPLES

    def test_fetchall_twice_returns_empty_second_time(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        assert cursor.fetchall() == _ROW_TUPLES
        assert cursor.fetchall() == []

    def test_fetchone_after_fetchall_returns_none(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        cursor.fetchall()
        assert cursor.fetchone() is None

    def test_fetchall_follows_pages_until_done(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket, rows=_ROWS[:1], is_done_on_fetch=False)
        _queue_page(socket, _ROWS[1:], is_done=True)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        assert cursor.fetchall() == _ROW_TUPLES

    def test_empty_page_without_is_done_stops_fetching(self, mock_sql_job):
        """A page with no rows ends the fetch even if the server never says is_done."""
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket, rows=[], is_done_on_fetch=False)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        assert cursor.fetchall() == []
        _queue_close(socket)
        cursor.close()


# ---------------------------------------------------------------------------
# buffering: rows are fetched in batches and never dropped
# ---------------------------------------------------------------------------

class TestFetchBuffering:
    def test_iteration_fetches_in_batches(self, mock_sql_job):
        """Iterating (fetchone under the hood) must not cost a round trip per row."""
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        assert list(cursor) == _ROW_TUPLES
        requests = _sqlmore_requests(socket)
        assert len(requests) == 1
        assert requests[0]["rows"] == cursor.fetch_size

    def test_rows_in_execute_response_are_returned(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_execute(socket, rows=_ROWS[:2], is_done=False)
        _queue_page(socket, _ROWS[2:], is_done=True)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        assert cursor.fetchall() == _ROW_TUPLES

    def test_complete_execute_response_needs_no_fetch(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_execute(socket, rows=_ROWS, is_done=True)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        assert cursor.fetchall() == _ROW_TUPLES
        assert _sqlmore_requests(socket) == []

    def test_fetchmany_serves_buffer_then_requests_only_the_shortfall(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket, rows=_ROWS[:2], is_done_on_fetch=False)
        _queue_page(socket, _ROWS[2:], is_done=True)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")

        assert cursor.fetchone() == _ROW_TUPLES[0]  # fetches a batch, buffers row 2
        assert cursor.fetchmany(5) == _ROW_TUPLES[1:]

        requests = _sqlmore_requests(socket)
        assert [r["rows"] for r in requests] == [cursor.fetch_size, 4]


# ---------------------------------------------------------------------------
# nextset(): switching to an earlier query uses that query's metadata
# ---------------------------------------------------------------------------

_DEPT_METADATA = {
    "column_count": 1,
    "job": "TEST/QUSER/JOB001",
    "columns": [{"name": "DEPTNO", "label": "DEPTNO", "type": "CHAR", "display_size": 3}],
}


class TestNextset:
    def test_switched_query_uses_its_own_metadata(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_execute(socket, metadata=_DEPT_METADATA)
        _queue_execute(socket)  # employee columns
        _queue_execute(socket, metadata=_DEPT_METADATA)
        cursor.execute("SELECT DEPTNO FROM SAMPLE.DEPARTMENT")
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        cursor.execute("SELECT DEPTNO FROM SAMPLE.DEPARTMENT")

        assert cursor.nextset() is True  # now on the employee query
        assert [col[0] for col in cursor.description] == ["EMPNO", "FIRSTNME", "SALARY"]
        _queue_page(socket, _ROWS, is_done=True)
        assert cursor.fetchall() == _ROW_TUPLES
        _queue_close(socket)  # the last department query is still open
        cursor.close()


# ---------------------------------------------------------------------------
# rowcount
# ---------------------------------------------------------------------------

class TestRowcount:
    def test_minus_one_before_execute(self, mock_sql_job):
        cursor, _ = _make_cursor(mock_sql_job)
        assert cursor.rowcount == -1

    def test_minus_one_for_select(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_select(socket, update_count=-1)
        cursor.execute("SELECT * FROM SAMPLE.EMPLOYEE")
        assert cursor.rowcount == -1

    def test_reflects_dml_update_count(self, mock_sql_job):
        cursor, socket = _make_cursor(mock_sql_job)
        _queue_dml(socket, update_count=5)
        cursor.execute("DELETE FROM SAMPLE.EMPLOYEE WHERE BONUS > 1000")
        assert cursor.rowcount == 5
