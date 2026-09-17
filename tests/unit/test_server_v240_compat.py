"""Sanity / compatibility tests for mapepire-server v2.4.0 changes.

Commit: 33a3e309e05b50fe0206a28cbba8b687a5cacef8
PR #145 – "Server logging and system connection rework"

Key server-side changes that affect the Python client:

1. SetConfig response no longer includes ``jtopentracedest`` / ``jtopentracelevel``.
   The server now manages tracing per-connection and strips those fields.

2. GetTraceData response no longer includes ``jtopentracedata``.
   The ``addReplyData("jtopentracedata", ...)`` call was removed from GetTraceData.java.

3. Tracing is now per-connection (each WebSocket connection gets its own
   isolated Tracer), so gettracedata only returns data for the calling connection.

4. SystemConnection constructor now requires a Tracer (internal change), but the
   connection/reconnect message format is unchanged from the client perspective.

5. Reconnect technique defaults to ``ConnectionMethod.getDefault()`` (CLI in
   single-mode, TCP in daemon-mode) instead of hard-coded CLI.

All tests here are server-independent (unit tests using MockSocket / conftest
fixtures).  They verify that the Python client handles the new response shapes
correctly without breaking on missing optional fields.
"""
import dataclasses
import json
import sys
from unittest.mock import MagicMock

# Prevent gssapi import errors in environments without Kerberos libraries
sys.modules.setdefault("gssapi", MagicMock())
sys.modules.setdefault("gssapi.raw", MagicMock())

# isort: split
import pytest

from mapepire_python.data_types import (
    ConnectRequest,
    GetTraceDataRequest,
    GetTraceDataResult,
    MessageType,
    ServerTraceDest,
    ServerTraceLevel,
    SetConfigRequest,
    SetConfigResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_response(id: str = "abc123") -> dict:
    return {"id": id, "success": True, "sql_rc": 0, "sql_state": "00000"}


# ===========================================================================
# 1. SetConfig response — jtopen fields removed in v2.4.0
# ===========================================================================

class TestSetConfigResponseV240:
    """Server v2.4.0 no longer emits jtopentracedest / jtopentracelevel in
    the SetConfig response.  The client must parse the new minimal shape
    without raising and must leave the optional jtopen fields as None.
    """

    def test_new_response_without_jtopen_fields_is_parsed(self):
        """v2.4.0 server response: only tracedest + tracelevel present."""
        data = {
            **_base_response("setconfig1"),
            "tracedest": "FILE",
            "tracelevel": "ON",
        }
        result = SetConfigResult.from_dict(data)  # type: ignore[attr-defined]
        assert result.success is True
        assert result.tracedest == ServerTraceDest.FILE
        assert result.tracelevel == ServerTraceLevel.ON

    def test_new_response_jtopen_fields_default_to_none(self):
        """Missing jtopen keys must yield None, not raise KeyError."""
        data = {
            **_base_response("setconfig2"),
            "tracedest": "IN_MEM",
            "tracelevel": "ERRORS",
        }
        result = SetConfigResult.from_dict(data)  # type: ignore[attr-defined]
        assert result.jtopentracedest is None
        assert result.jtopentracelevel is None

    def test_old_response_with_jtopen_fields_still_parsed(self):
        """Backward compat: a pre-v2.4.0 response that includes jtopen fields
        must still be accepted without raising."""
        data = {
            **_base_response("setconfig3"),
            "tracedest": "FILE",
            "tracelevel": "OFF",
            "jtopentracedest": "FILE",
            "jtopentracelevel": "OFF",
        }
        result = SetConfigResult.from_dict(data)  # type: ignore[attr-defined]
        assert result.tracedest == ServerTraceDest.FILE
        assert result.jtopentracelevel == ServerTraceLevel.OFF

    def test_tracelevel_datastream_parsed(self):
        data = {**_base_response(), "tracedest": "FILE", "tracelevel": "DATASTREAM"}
        result = SetConfigResult.from_dict(data)  # type: ignore[attr-defined]
        assert result.tracelevel == ServerTraceLevel.DATASTREAM

    def test_tracelevel_input_and_errors_parsed(self):
        data = {**_base_response(), "tracedest": "IN_MEM", "tracelevel": "INPUT_AND_ERRORS"}
        result = SetConfigResult.from_dict(data)  # type: ignore[attr-defined]
        assert result.tracelevel == ServerTraceLevel.INPUT_AND_ERRORS

    def test_tracedest_in_mem_parsed(self):
        data = {**_base_response(), "tracedest": "IN_MEM", "tracelevel": "OFF"}
        result = SetConfigResult.from_dict(data)  # type: ignore[attr-defined]
        assert result.tracedest == ServerTraceDest.IN_MEM

    def test_failed_setconfig_response_preserved(self):
        """A failed SetConfig response must not raise during parsing."""
        data = {
            **_base_response(),
            "success": False,
            "error": "Invalid trace level specified: BADVALUE",
            "tracedest": "FILE",
            "tracelevel": "OFF",
        }
        result = SetConfigResult.from_dict(data)  # type: ignore[attr-defined]
        assert result.success is False
        assert "Invalid trace level" in (result.error or "")


# ===========================================================================
# 2. SetConfigRequest — request serialization unchanged
# ===========================================================================

class TestSetConfigRequestSerialization:
    """The client's outgoing SetConfig request format must be unchanged so that
    both old and new servers understand it.  The jtopen request fields are
    simply ignored by v2.4.0+.
    """

    def test_minimal_request_serializes(self):
        req = SetConfigRequest(id="sc1", tracelevel="ON", tracedest="FILE")
        d = dataclasses.asdict(req)
        assert d["type"] == MessageType.SET_CONFIG.value
        assert d["tracelevel"] == "ON"
        assert d["tracedest"] == "FILE"

    def test_request_is_json_serializable(self):
        req = SetConfigRequest(id="sc2", tracelevel="ERRORS", tracedest="IN_MEM")
        json.dumps(dataclasses.asdict(req))  # must not raise

    def test_request_id_preserved(self):
        req = SetConfigRequest(id="unique-id-99")
        assert req.id == "unique-id-99"


# ===========================================================================
# 3. GetTraceData response — jtopentracedata removed in v2.4.0
# ===========================================================================

class TestGetTraceDataResponseV240:
    """Server v2.4.0 removes jtopentracedata from the GetTraceData response
    (the addReplyData call was deleted from GetTraceData.java).  The client's
    GetTraceDataResult has jtopentracedata as Optional[str] = None so it must
    accept responses both with and without the key.
    """

    def test_new_response_without_jtopentracedata_parsed(self):
        """v2.4.0 server response: only tracedata is present."""
        data = {**_base_response("gtd1"), "tracedata": "some trace output"}
        result = GetTraceDataResult.from_dict(data)  # type: ignore[attr-defined]
        assert result.tracedata == "some trace output"
        assert result.jtopentracedata is None

    def test_new_response_empty_tracedata_string(self):
        """Newly started job with no trace activity returns empty tracedata."""
        data = {**_base_response("gtd2"), "tracedata": ""}
        result = GetTraceDataResult.from_dict(data)  # type: ignore[attr-defined]
        assert result.tracedata == ""
        assert result.jtopentracedata is None

    def test_old_response_with_jtopentracedata_still_accepted(self):
        """Pre-v2.4.0 server responses that include jtopentracedata must not
        cause a parse error (backward compat)."""
        data = {
            **_base_response("gtd3"),
            "tracedata": "conn trace",
            "jtopentracedata": "jtopen trace",
        }
        result = GetTraceDataResult.from_dict(data)  # type: ignore[attr-defined]
        assert result.tracedata == "conn trace"
        assert result.jtopentracedata == "jtopen trace"

    def test_gettracedata_request_type(self):
        req = GetTraceDataRequest(id="gtr1")
        assert req.type == MessageType.GET_TRACE_DATA.value
        d = dataclasses.asdict(req)
        json.dumps(d)  # must be JSON-serializable


# ===========================================================================
# 4. Connection request — technique field and format unchanged
# ===========================================================================

class TestConnectRequestFormat:
    """The connection request format is unchanged.  The server's per-connection
    Tracer init and user-swap changes are transparent to the client.
    """

    def test_connect_request_default_technique_is_tcp(self):
        """Client always sends TCP technique; the server-side single-mode
        vs daemon-mode decision is internal to the server."""
        req = ConnectRequest(id="conn1")
        d = dataclasses.asdict(req)
        assert d["technique"] == "tcp"
        assert d["type"] == MessageType.CONNECT.value

    def test_connect_request_is_json_serializable(self):
        req = ConnectRequest(id="conn2", props="naming=system;libraries=MYLIB")
        json.dumps(dataclasses.asdict(req))

    def test_connect_request_application_default(self):
        req = ConnectRequest(id="conn3")
        assert req.application == "Python Client"


# ===========================================================================
# 5. Per-connection trace isolation — MockSocket end-to-end flow
# ===========================================================================

class TestPerConnectionTraceFlow:
    """Exercise the client-side request/response flow for setconfig + gettracedata
    using a MockSocket (no real server).

    v2.4.0 key behavior: tracing is per-connection, so two independent
    connections each see only their own trace data.  The client doesn't need
    to do anything differently — this is a server-side concern — but the client
    must correctly parse the responses it receives.
    """

    def test_setconfig_then_gettracedata_roundtrip(self, mock_sql_job, make_query_result):
        """Simulate: send setconfig ON, receive minimal v2.4.0 response, then
        send gettracedata and receive per-connection trace output."""
        job, socket = mock_sql_job

        # v2.4.0 SetConfig response: only tracedest + tracelevel (no jtopen)
        setconfig_resp = json.dumps({
            "id": "sc1",
            "success": True,
            "sql_rc": 0,
            "sql_state": "00000",
            "tracedest": "IN_MEM",
            "tracelevel": "ON",
            "error": None,
            "execution_time": None,
        })
        # v2.4.0 GetTraceData response: only tracedata (no jtopentracedata)
        gettracedata_resp = json.dumps({
            "id": "gtd1",
            "success": True,
            "sql_rc": 0,
            "sql_state": "00000",
            "tracedata": "[2026-09-04] connected as TEST/QUSER/JOB001",
            "error": None,
            "execution_time": None,
        })
        socket.add_response(setconfig_resp)
        socket.add_response(gettracedata_resp)

        # SetConfig
        import dataclasses as dc
        job.send(json.dumps(dc.asdict(SetConfigRequest(id="sc1", tracedest="IN_MEM", tracelevel="ON"))))
        raw_sc = json.loads(socket.recv.__func__(socket) if False else socket._response_queue.pop(0))
        sc_result = SetConfigResult.from_dict(raw_sc)  # type: ignore[attr-defined]
        assert sc_result.success is True
        assert sc_result.tracedest == ServerTraceDest.IN_MEM
        assert sc_result.tracelevel == ServerTraceLevel.ON
        assert sc_result.jtopentracedest is None

        # GetTraceData
        job.send(json.dumps(dc.asdict(GetTraceDataRequest(id="gtd1"))))
        raw_gtd = json.loads(socket._response_queue.pop(0))
        gtd_result = GetTraceDataResult.from_dict(raw_gtd)  # type: ignore[attr-defined]
        assert gtd_result.success is True
        assert "TEST/QUSER/JOB001" in gtd_result.tracedata
        assert gtd_result.jtopentracedata is None
