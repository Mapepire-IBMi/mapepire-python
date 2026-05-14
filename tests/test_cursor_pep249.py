"""Unit tests for PEP 249 cursor compliance (no live server required).

Covers:
  - cursor.description: 7-tuple structure, field mapping, pre/post execute state
  - fetchone() / fetchall() / fetchmany(): return tuples not dicts, correct values
  - fetchone() returns None when exhausted
  - rowcount: -1 for SELECT, actual count for DML
"""
import json

import pytest

from mapepire_python.core.cursor import Cursor


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


def _queue_select(socket, rows=None, update_count=-1, metadata=None, is_done_on_fetch=True):
    """Queue the two socket responses needed for execute() + one fetch call."""
    # 1. prepare_sql_execute response: metadata present, no data rows yet
    socket.add_response(json.dumps({
        "id": "q1",
        "success": True,
        "sql_rc": 0,
        "sql_state": "",
        "is_done": False,
        "has_results": True,
        "update_count": update_count,
        "data": [],
        "metadata": metadata if metadata is not None else _METADATA,
        "error": None,
        "execution_time": None,
    }))
    # 2. sqlmore response: actual data rows
    socket.add_response(json.dumps({
        "id": "q1",
        "success": True,
        "sql_rc": 0,
        "sql_state": "",
        "is_done": is_done_on_fetch,
        "data": rows if rows is not None else _ROWS,
        "error": None,
        "execution_time": None,
    }))


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
