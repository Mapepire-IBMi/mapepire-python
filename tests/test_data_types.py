"""Unit tests for data_types.py — serialization/deserialization round-trips.

These tests do not require a server connection.
"""
import sys
from unittest.mock import MagicMock

sys.modules['gssapi'] = MagicMock()
sys.modules['gssapi.raw'] = MagicMock()
import dataclasses
import json

import pytest

from mapepire_python.data_types import (
    BadRequestError,
    CLCommandResult,
    ColumnMetaData,
    ConnectionResult,
    ExitResponse,
    GetDbJobResponse,
    GetTraceDataResult,
    IncompleteError,
    JobLogEntry,
    MessageType,
    ConnectionTechnique,
    ParameterDetail,
    ParameterMode,
    ParameterResult,
    PingResponse,
    PrepareSqlResponse,
    QueryMetaData,
    QueryResult,
    ServerTraceDest,
    ServerTraceLevel,
    SetConfigResult,
    SqlCloseResponse,
    SqlMoreResponse,
    UnknownError,
    UnparsableError,
    VersionCheckResult,
    ConnectRequest,
    SqlRequest,
    PrepareSqlRequest,
    PrepareSqlExecuteRequest,
    ExecuteRequest,
    SqlMoreRequest,
    SqlCloseRequest,
    ClRequest,
    PingRequest,
    GetDbJobRequest,
    GetVersionRequest,
    SetConfigRequest,
    GetTraceDataRequest,
    ExitRequest,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_response_fields():
    return {"id": "abc123", "success": True, "sql_rc": 0, "sql_state": "00000"}


# ---------------------------------------------------------------------------
# Enum tests
# ---------------------------------------------------------------------------

class TestEnums:
    def test_message_type_values(self):
        assert MessageType.CONNECT.value == "connect"
        assert MessageType.SQL.value == "sql"
        assert MessageType.EXIT.value == "exit"
        assert len(MessageType) == 14

    def test_connection_technique_values(self):
        assert ConnectionTechnique.TCP.value == "tcp"
        assert ConnectionTechnique.CLI.value == "cli"

    def test_parameter_mode_values(self):
        assert ParameterMode.IN.value == "IN"
        assert ParameterMode.OUT.value == "OUT"
        assert ParameterMode.INOUT.value == "INOUT"
        assert ParameterMode.UNKNOWN.value == "UNKNOWN"

    def test_server_trace_level_has_input_and_errors(self):
        assert ServerTraceLevel.INPUT_AND_ERRORS.value == "INPUT_AND_ERRORS"
        assert len(ServerTraceLevel) == 5

    def test_server_trace_dest_values(self):
        assert ServerTraceDest.FILE.value == "FILE"
        assert ServerTraceDest.IN_MEM.value == "IN_MEM"


# ---------------------------------------------------------------------------
# Request type tests (serialization via dataclasses.asdict)
# ---------------------------------------------------------------------------

class TestRequestTypes:
    def test_connect_request_type_field(self):
        req = ConnectRequest(id="1")
        assert req.type == MessageType.CONNECT.value
        assert req.technique == ConnectionTechnique.TCP.value

    def test_sql_request_type_field(self):
        req = SqlRequest(id="2", sql="SELECT 1 FROM SYSIBM.SYSDUMMY1", rows=10)
        assert req.type == MessageType.SQL.value
        assert req.sql == "SELECT 1 FROM SYSIBM.SYSDUMMY1"

    def test_prepare_sql_request(self):
        req = PrepareSqlRequest(id="3", sql="SELECT ? FROM SYSIBM.SYSDUMMY1")
        assert req.type == MessageType.PREPARE_SQL.value

    def test_prepare_sql_execute_request(self):
        req = PrepareSqlExecuteRequest(id="4", sql="SELECT ? FROM SYSIBM.SYSDUMMY1", rows=5)
        assert req.type == MessageType.PREPARE_SQL_EXECUTE.value

    def test_execute_request(self):
        req = ExecuteRequest(id="5", cont_id="stmt-1", rows=10, parameters=["value"])
        assert req.type == MessageType.EXECUTE.value
        d = dataclasses.asdict(req)
        assert d["cont_id"] == "stmt-1"
        assert d["parameters"] == ["value"]

    def test_sql_more_request(self):
        req = SqlMoreRequest(id="6", cont_id="stmt-1", rows=100)
        assert req.type == MessageType.SQL_MORE.value

    def test_sql_close_request(self):
        req = SqlCloseRequest(id="7", cont_id="stmt-1")
        assert req.type == MessageType.SQL_CLOSE.value

    def test_cl_request(self):
        req = ClRequest(id="8", cmd="WRKACTJOB")
        assert req.type == MessageType.CL.value

    def test_ping_request(self):
        req = PingRequest(id="9")
        assert req.type == MessageType.PING.value

    def test_get_db_job_request(self):
        req = GetDbJobRequest(id="10")
        assert req.type == MessageType.GET_DB_JOB.value

    def test_get_version_request(self):
        req = GetVersionRequest(id="11")
        assert req.type == MessageType.GET_VERSION.value

    def test_set_config_request(self):
        req = SetConfigRequest(id="12", tracelevel="ON", tracedest="FILE")
        assert req.type == MessageType.SET_CONFIG.value

    def test_get_trace_data_request(self):
        req = GetTraceDataRequest(id="13")
        assert req.type == MessageType.GET_TRACE_DATA.value

    def test_exit_request(self):
        req = ExitRequest(id="14")
        assert req.type == MessageType.EXIT.value

    def test_request_serialization_roundtrip(self):
        req = SqlRequest(id="x", sql="SELECT 1 FROM SYSIBM.SYSDUMMY1", rows=5, terse=True)
        d = dataclasses.asdict(req)
        assert d["id"] == "x"
        assert d["sql"] == "SELECT 1 FROM SYSIBM.SYSDUMMY1"
        assert d["rows"] == 5
        assert d["terse"] is True
        assert d["type"] == MessageType.SQL.value
        # Verify JSON serializable
        json.dumps(d)


# ---------------------------------------------------------------------------
# Response type tests (from_dict / to_dict round-trips)
# ---------------------------------------------------------------------------

class TestConnectionResult:
    def test_from_dict(self):
        data = {**_base_response_fields(), "job": "123456/QUSER/QZDASOINIT"}
        result = ConnectionResult.from_dict(data)
        assert result.job == "123456/QUSER/QZDASOINIT"
        assert result.success is True

    def test_from_dict_with_error(self):
        data = {**_base_response_fields(), "job": "", "success": False, "error": "Auth failed"}
        result = ConnectionResult.from_dict(data)
        assert result.success is False
        assert result.error == "Auth failed"

    def test_roundtrip(self):
        data = {**_base_response_fields(), "job": "123456/QUSER/QZDASOINIT"}
        result = ConnectionResult.from_dict(data)
        d = result.to_dict()
        assert d["job"] == "123456/QUSER/QZDASOINIT"


class TestQueryResult:
    def test_from_dict_basic(self):
        data = {"is_done": True, "has_results": True, "update_count": 0, "data": [{"A": 1}]}
        result = QueryResult.from_dict(data)
        assert result.is_done is True
        assert result.data == [{"A": 1}]

    def test_from_dict_with_parameter_count(self):
        data = {"is_done": True, "has_results": False, "update_count": 1, "data": [], "parameter_count": 2}
        result = QueryResult.from_dict(data)
        assert result.parameter_count == 2

    def test_roundtrip(self):
        data = {"is_done": False, "has_results": True, "update_count": 0, "data": [{"X": 42}]}
        result = QueryResult.from_dict(data)
        d = result.to_dict()
        assert d["data"] == [{"X": 42}]
        assert d["is_done"] is False


class TestPingResponse:
    def test_from_dict(self):
        data = {**_base_response_fields(), "alive": True, "db_alive": True}
        result = PingResponse.from_dict(data)
        assert result.alive is True
        assert result.db_alive is True

    def test_from_dict_defaults(self):
        result = PingResponse.from_dict(_base_response_fields())
        assert result.alive is None
        assert result.db_alive is None


class TestPrepareSqlResponse:
    def test_from_dict(self):
        data = {**_base_response_fields(), "parameter_count": 3}
        result = PrepareSqlResponse.from_dict(data)
        assert result.parameter_count == 3


class TestSqlMoreResponse:
    def test_from_dict(self):
        data = {**_base_response_fields(), "data": [{"col": "val"}], "is_done": False}
        result = SqlMoreResponse.from_dict(data)
        assert result.data == [{"col": "val"}]
        assert result.is_done is False

    def test_from_dict_done(self):
        data = {**_base_response_fields(), "data": [], "is_done": True}
        result = SqlMoreResponse.from_dict(data)
        assert result.is_done is True


class TestSqlCloseResponse:
    def test_from_dict(self):
        result = SqlCloseResponse.from_dict(_base_response_fields())
        assert result.success is True


class TestGetDbJobResponse:
    def test_from_dict(self):
        data = {**_base_response_fields(), "job": "111111/QUSER/QZDASOINIT"}
        result = GetDbJobResponse.from_dict(data)
        assert result.job == "111111/QUSER/QZDASOINIT"

    def test_from_dict_default_job(self):
        result = GetDbJobResponse.from_dict(_base_response_fields())
        assert result.job == ""


class TestExitResponse:
    def test_from_dict(self):
        result = ExitResponse.from_dict(_base_response_fields())
        assert result.success is True


class TestVersionCheckResult:
    def test_from_dict(self):
        data = {**_base_response_fields(), "build_date": "2024-01-01", "version": "1.0.0"}
        result = VersionCheckResult.from_dict(data)
        assert result.version == "1.0.0"
        assert result.build_date == "2024-01-01"


class TestGetTraceDataResult:
    def test_from_dict(self):
        data = {**_base_response_fields(), "tracedata": "trace output", "jtopentracedata": "jtopen trace"}
        result = GetTraceDataResult.from_dict(data)
        assert result.tracedata == "trace output"
        assert result.jtopentracedata == "jtopen trace"

    def test_from_dict_without_jtopen(self):
        data = {**_base_response_fields(), "tracedata": "trace output"}
        result = GetTraceDataResult.from_dict(data)
        assert result.jtopentracedata is None


# ---------------------------------------------------------------------------
# Shared data structure tests
# ---------------------------------------------------------------------------

class TestColumnMetaData:
    def test_required_fields(self):
        col = ColumnMetaData(display_size=10, label="Name", name="NAME", type="VARCHAR")
        assert col.name == "NAME"
        assert col.precision is None
        assert col.nullable is None

    def test_all_fields(self):
        col = ColumnMetaData(
            display_size=10, label="ID", name="ID", type="INTEGER",
            precision=10, scale=0, autoIncrement=True,
            nullable=False, readOnly=False, writeable=True, table="MYTABLE"
        )
        assert col.precision == 10
        assert col.autoIncrement is True
        assert col.table == "MYTABLE"


class TestParameterDetail:
    def test_basic(self):
        p = ParameterDetail(name="P1", type="INTEGER", mode=ParameterMode.IN)
        assert p.mode == ParameterMode.IN
        assert p.precision is None

    def test_with_precision(self):
        p = ParameterDetail(name="P2", type="DECIMAL", mode=ParameterMode.INOUT, precision=10, scale=2)
        assert p.precision == 10
        assert p.scale == 2


class TestParameterResult:
    def test_basic(self):
        pr = ParameterResult(index=0, type="INTEGER", precision=10, scale=0, name="OUT1", value=42)
        assert pr.value == 42
        assert pr.index == 0

    def test_optional_fields(self):
        pr = ParameterResult(index=1, type="VARCHAR", precision=50, scale=0, name="OUT2")
        assert pr.value is None
        assert pr.ccsid is None


class TestJobLogEntry:
    def test_severity_is_int(self):
        entry = JobLogEntry(
            MESSAGE_ID="CPF0001", SEVERITY=30,
            MESSAGE_TIMESTAMP="2024-01-01T00:00:00",
            FROM_LIBRARY="QSYS", FROM_PROGRAM="QCMD",
            MESSAGE_TYPE="INFORMATIONAL",
            MESSAGE_TEXT="Job ended normally.",
            MESSAGE_SECOND_LEVEL_TEXT=""
        )
        assert isinstance(entry.SEVERITY, int)
        assert entry.SEVERITY == 30


# ---------------------------------------------------------------------------
# Error type tests
# ---------------------------------------------------------------------------

class TestProtocolErrorTypes:
    def test_unparsable_error_from_dict(self):
        data = {**_base_response_fields(), "success": False, "error": "Could not parse message"}
        err = UnparsableError.from_dict(data)
        assert err.success is False
        assert err.error == "Could not parse message"

    def test_incomplete_error_from_dict(self):
        data = {**_base_response_fields(), "success": False, "error": "Incomplete message"}
        err = IncompleteError.from_dict(data)
        assert err.error == "Incomplete message"

    def test_unknown_error_from_dict(self):
        data = {**_base_response_fields(), "success": False, "error": "Unknown error"}
        err = UnknownError.from_dict(data)
        assert err.error == "Unknown error"

    def test_bad_request_error_from_dict(self):
        data = {**_base_response_fields(), "success": False, "error": "Bad request"}
        err = BadRequestError.from_dict(data)
        assert err.error == "Bad request"

    def test_error_types_are_server_response_subclasses(self):
        from mapepire_python.data_types import ServerResponse
        for cls in (UnparsableError, IncompleteError, UnknownError, BadRequestError):
            assert issubclass(cls, ServerResponse)
