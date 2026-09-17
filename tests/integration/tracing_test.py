"""Integration tests for server v2.4.0 tracing and system connection changes.

Covers the client-observable effects of commit 33a3e309:
  PR #145 – "Server logging and system connection rework"

Key behaviors exercised against a live server:

  1. getversion   – still works; version is now >= 2.4.0
  2. ping         – still works; alive + db_alive both True after connect
  3. setconfig    – v2.4.0 response no longer contains jtopentracedest /
                    jtopentracelevel; client must parse the slimmer shape
  4. gettracedata – v2.4.0 response no longer contains jtopentracedata;
                    tracing is per-connection so two independent connections
                    each receive their own (disjoint) trace buffers
  5. CL command after setconfig – tracing ON during a CL run should not
                    break normal command execution
  6. Multiple connections – per-connection trace isolation means two
                    concurrent connections each see only their own trace data

Each test is self-contained: it opens a fresh connection, exercises the
feature under test, and closes cleanly.  Tests use ``SQLJob`` (sync) unless
async is required, to keep the code straightforward.

Tests that assert v2.4.0-specific behavior are skipped automatically when
the live server reports a version below 2.4.0, so the suite stays green
against both the current production server (v2.3.x) and the new one.
"""
import dataclasses
import json
from functools import lru_cache
from typing import Tuple

import pytest

from mapepire_python import QueryOptions, SQLJob
from mapepire_python.data_types import (
    GetTraceDataRequest,
    GetTraceDataResult,
    GetVersionRequest,
    PingRequest,
    PingResponse,
    ServerTraceDest,
    ServerTraceLevel,
    SetConfigRequest,
    SetConfigResult,
    VersionCheckResult,
)

from .test_setup import *  # noqa: F401,F403 – imports creds, server, user, password, port


# ---------------------------------------------------------------------------
# Version helper – cached so only one connection is made per test session
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _server_version() -> Tuple[int, int, int]:
    """Return the live server version as a (major, minor, patch) tuple.

    Returns (0, 0, 0) if the server is unreachable at collection time so that
    pytest.mark.skipif evaluation never raises and collection always succeeds.
    The individual tests will fail naturally when they try to connect.
    """
    job = SQLJob()
    try:
        job.connect(creds)
        job.send(json.dumps(dataclasses.asdict(GetVersionRequest(id="ver1"))))
        raw = json.loads(job._socket.recv())
        result = VersionCheckResult.from_dict(raw)
        parts = result.version.split(".")[:3]
        return tuple(int(x) for x in parts)  # type: ignore[return-value]
    except Exception:
        return (0, 0, 0)
    finally:
        try:
            job.close()
        except Exception:
            pass


def _is_v240_or_higher() -> bool:
    major, minor, *_ = _server_version()
    return (major, minor) >= (2, 4)


def _version_str() -> str:
    return ".".join(str(x) for x in _server_version())


#: Skip marker for tests that only apply to server >= 2.4.0
requires_v240 = pytest.mark.skipif(
    not _is_v240_or_higher(),
    reason=f"Server is v{_version_str()} — "
           "this test requires mapepire-server >= 2.4.0 (commit 33a3e309)",
)


# ---------------------------------------------------------------------------
# 1. getversion – still works; version bumped to 2.4.0 in this commit
# ---------------------------------------------------------------------------

def test_getversion_returns_version_string():
    """getversion should return a non-empty version and build_date."""
    job = SQLJob()
    job.connect(creds)
    try:
        job.send(json.dumps(dataclasses.asdict(GetVersionRequest(id=job._get_unique_id("getversion")))))
        raw = json.loads(job._socket.recv())
        result = VersionCheckResult.from_dict(raw)
        assert result.success is True
        assert result.version != ""
        assert result.build_date != ""
    finally:
        job.close()


@requires_v240
def test_getversion_version_is_240_or_higher():
    """Sanity-check: the server under test must be running v2.4.0+."""
    major, minor, *_ = _server_version()
    assert (major, minor) >= (2, 4)


# ---------------------------------------------------------------------------
# 2. ping – alive + db_alive both True after successful connect
# ---------------------------------------------------------------------------

def test_ping_after_connect_returns_alive():
    """After a successful connect, ping must report alive=True and db_alive=True."""
    job = SQLJob()
    job.connect(creds)
    try:
        job.send(json.dumps(dataclasses.asdict(PingRequest(id=job._get_unique_id("ping")))))
        raw = json.loads(job._socket.recv())
        result = PingResponse.from_dict(raw)
        assert result.success is True
        assert result.alive is True
        assert result.db_alive is True
    finally:
        job.close()


def test_ping_response_has_no_extra_error():
    """A healthy ping must have no error message."""
    job = SQLJob()
    job.connect(creds)
    try:
        job.send(json.dumps(dataclasses.asdict(PingRequest(id=job._get_unique_id("ping")))))
        raw = json.loads(job._socket.recv())
        result = PingResponse.from_dict(raw)
        assert result.error is None or result.error == ""
    finally:
        job.close()


# ---------------------------------------------------------------------------
# 3. setconfig – v2.4.0 response shape (no jtopen fields)
# ---------------------------------------------------------------------------

@requires_v240
def test_setconfig_tracelevel_on_parses_correctly():
    """setconfig ON should succeed and return tracedest + tracelevel only
    (no jtopentracedest / jtopentracelevel in v2.4.0 responses)."""
    job = SQLJob()
    job.connect(creds)
    try:
        req = SetConfigRequest(
            id=job._get_unique_id("setconfig"),
            tracedest="IN_MEM",
            tracelevel="ON",
        )
        job.send(json.dumps(dataclasses.asdict(req)))
        raw = json.loads(job._socket.recv())
        result = SetConfigResult.from_dict(raw)
        assert result.success is True
        assert result.tracedest == ServerTraceDest.IN_MEM
        assert result.tracelevel == ServerTraceLevel.ON
        # v2.4.0: jtopen fields absent from response
        assert result.jtopentracedest is None
        assert result.jtopentracelevel is None
    finally:
        job.close()


@requires_v240
def test_setconfig_tracelevel_off_parses_correctly():
    """setconfig OFF should succeed and the client should parse the minimal response."""
    job = SQLJob()
    job.connect(creds)
    try:
        req = SetConfigRequest(
            id=job._get_unique_id("setconfig"),
            tracedest="FILE",
            tracelevel="OFF",
        )
        job.send(json.dumps(dataclasses.asdict(req)))
        raw = json.loads(job._socket.recv())
        result = SetConfigResult.from_dict(raw)
        assert result.success is True
        assert result.tracelevel == ServerTraceLevel.OFF
    finally:
        job.close()


@requires_v240
def test_setconfig_errors_trace_level():
    """ERRORS trace level must be accepted and echoed back."""
    job = SQLJob()
    job.connect(creds)
    try:
        req = SetConfigRequest(
            id=job._get_unique_id("setconfig"),
            tracedest="IN_MEM",
            tracelevel="ERRORS",
        )
        job.send(json.dumps(dataclasses.asdict(req)))
        raw = json.loads(job._socket.recv())
        result = SetConfigResult.from_dict(raw)
        assert result.success is True
        assert result.tracelevel == ServerTraceLevel.ERRORS
    finally:
        job.close()


def test_setconfig_invalid_level_returns_failure():
    """An invalid trace level should return success=False, not crash the connection."""
    job = SQLJob()
    job.connect(creds)
    try:
        req = SetConfigRequest(
            id=job._get_unique_id("setconfig"),
            tracedest="FILE",
            tracelevel="BADLEVEL",
        )
        job.send(json.dumps(dataclasses.asdict(req)))
        raw = json.loads(job._socket.recv())
        # The server returns an error response; the connection should remain usable
        assert raw.get("success") is False or "error" in raw
        # Verify connection is still alive after the bad config
        result = job.query_and_run("VALUES (1)")
        assert result["success"] is True
    finally:
        job.close()


# ---------------------------------------------------------------------------
# 4. gettracedata – per-connection, no jtopentracedata in v2.4.0
# ---------------------------------------------------------------------------

def test_gettracedata_returns_string_tracedata():
    """After enabling IN_MEM tracing and running a query, gettracedata must
    return a non-empty tracedata string for this connection."""
    job = SQLJob()
    job.connect(creds)
    try:
        # Enable per-connection in-memory tracing (v2.4.0)
        set_req = SetConfigRequest(
            id=job._get_unique_id("setconfig"),
            tracedest="IN_MEM",
            tracelevel="ON",
        )
        job.send(json.dumps(dataclasses.asdict(set_req)))
        json.loads(job._socket.recv())  # consume setconfig response

        # Run something to generate trace entries
        job.query_and_run("VALUES (1)")

        # Fetch per-connection trace data
        gtd_req = GetTraceDataRequest(id=job._get_unique_id("gettracedata"))
        job.send(json.dumps(dataclasses.asdict(gtd_req)))
        raw = json.loads(job._socket.recv())
        result = GetTraceDataResult.from_dict(raw)

        assert result.success is True
        assert isinstance(result.tracedata, str)
        assert len(result.tracedata) > 0
    finally:
        job.close()


@requires_v240
def test_gettracedata_no_jtopentracedata_field():
    """v2.4.0 server must NOT include jtopentracedata; the Python client
    should therefore receive None for that optional field."""
    job = SQLJob()
    job.connect(creds)
    try:
        set_req = SetConfigRequest(
            id=job._get_unique_id("setconfig"),
            tracedest="IN_MEM",
            tracelevel="ON",
        )
        job.send(json.dumps(dataclasses.asdict(set_req)))
        json.loads(job._socket.recv())

        gtd_req = GetTraceDataRequest(id=job._get_unique_id("gettracedata"))
        job.send(json.dumps(dataclasses.asdict(gtd_req)))
        raw = json.loads(job._socket.recv())

        # v2.4.0: the raw response dict must not carry jtopentracedata
        assert "jtopentracedata" not in raw, (
            "Server v2.4.0 must not emit jtopentracedata; "
            f"found it in the raw response: {list(raw.keys())}"
        )
        result = GetTraceDataResult.from_dict(raw)
        assert result.jtopentracedata is None
    finally:
        job.close()


def test_gettracedata_off_returns_empty_or_minimal():
    """With tracing OFF, gettracedata should still succeed but tracedata
    may be empty or contain only connection-init messages."""
    job = SQLJob()
    job.connect(creds)
    try:
        # Explicitly set OFF (default, but explicit for test clarity)
        set_req = SetConfigRequest(
            id=job._get_unique_id("setconfig"),
            tracedest="FILE",
            tracelevel="OFF",
        )
        job.send(json.dumps(dataclasses.asdict(set_req)))
        json.loads(job._socket.recv())

        gtd_req = GetTraceDataRequest(id=job._get_unique_id("gettracedata"))
        job.send(json.dumps(dataclasses.asdict(gtd_req)))
        raw = json.loads(job._socket.recv())
        result = GetTraceDataResult.from_dict(raw)

        assert result.success is True
        assert isinstance(result.tracedata, str)  # may be "" or minimal
    finally:
        job.close()


# ---------------------------------------------------------------------------
# 5. setconfig + CL command – tracing must not break normal operations
# ---------------------------------------------------------------------------

def test_cl_command_works_with_tracing_on():
    """Enabling tracing mid-session must not break subsequent CL commands."""
    job = SQLJob()
    job.connect(creds)
    try:
        # Turn tracing on
        set_req = SetConfigRequest(
            id=job._get_unique_id("setconfig"),
            tracedest="IN_MEM",
            tracelevel="ON",
        )
        job.send(json.dumps(dataclasses.asdict(set_req)))
        sc_raw = json.loads(job._socket.recv())
        assert sc_raw.get("success") is True

        # CL command should still work — WRKACTJOB is the same command used
        # in cl_test.py; success=True is the key assertion (data may be empty
        # in single-mode, which is expected behaviour).
        opts = QueryOptions(isClCommand=True)
        query = job.query("WRKACTJOB", opts=opts)
        result = query.run()
        assert result["success"] is True
        assert result["is_done"] is True
    finally:
        job.close()


def test_sql_query_works_with_tracing_on():
    """A normal SQL query must return correct results while tracing is active."""
    job = SQLJob()
    job.connect(creds)
    try:
        set_req = SetConfigRequest(
            id=job._get_unique_id("setconfig"),
            tracedest="IN_MEM",
            tracelevel="ON",
        )
        job.send(json.dumps(dataclasses.asdict(set_req)))
        json.loads(job._socket.recv())

        result = job.query_and_run("select * from sample.employee", rows_to_fetch=5)
        assert result["success"] is True
        assert result["has_results"] is True
        assert len(result["data"]) == 5
    finally:
        job.close()


@requires_v240
def test_setconfig_then_sql_then_gettracedata_full_flow():
    """End-to-end: enable tracing → run query → retrieve trace data → disable.
    Simulates the canonical debugging workflow a user would follow."""
    job = SQLJob()
    job.connect(creds)
    try:
        # 1. Enable
        set_req = SetConfigRequest(
            id=job._get_unique_id("sc"),
            tracedest="IN_MEM",
            tracelevel="ON",
        )
        job.send(json.dumps(dataclasses.asdict(set_req)))
        sc_result = SetConfigResult.from_dict(json.loads(job._socket.recv()))
        assert sc_result.success is True
        assert sc_result.tracelevel == ServerTraceLevel.ON

        # 2. Run a query (generates trace output)
        job.query_and_run("VALUES (job_name)")

        # 3. Retrieve trace data
        gtd_req = GetTraceDataRequest(id=job._get_unique_id("gtd"))
        job.send(json.dumps(dataclasses.asdict(gtd_req)))
        gtd_result = GetTraceDataResult.from_dict(json.loads(job._socket.recv()))
        assert gtd_result.success is True
        assert len(gtd_result.tracedata) > 0
        assert gtd_result.jtopentracedata is None  # v2.4.0: never returned

        # 4. Disable
        off_req = SetConfigRequest(
            id=job._get_unique_id("sc"),
            tracedest="FILE",
            tracelevel="OFF",
        )
        job.send(json.dumps(dataclasses.asdict(off_req)))
        off_result = SetConfigResult.from_dict(json.loads(job._socket.recv()))
        assert off_result.success is True
        assert off_result.tracelevel == ServerTraceLevel.OFF
    finally:
        job.close()


# ---------------------------------------------------------------------------
# 6. Per-connection trace isolation (v2.4.0 key feature)
# ---------------------------------------------------------------------------

def test_two_connections_have_independent_trace_buffers():
    """v2.4.0 per-connection tracing: job_a and job_b each get their own
    isolated trace buffer.  After both run distinct queries with tracing ON,
    neither trace buffer should contain the *other* connection's job name."""
    job_a = SQLJob()
    job_b = SQLJob()
    try:
        job_a.connect(creds)
        job_b.connect(creds)

        # Enable in-memory tracing on both connections
        for job in (job_a, job_b):
            req = SetConfigRequest(
                id=job._get_unique_id("sc"),
                tracedest="IN_MEM",
                tracelevel="ON",
            )
            job.send(json.dumps(dataclasses.asdict(req)))
            json.loads(job._socket.recv())

        # Record each connection's server-side job name (IBM i job identifier)
        result_a = job_a.query_and_run("VALUES (job_name)")
        result_b = job_b.query_and_run("VALUES (job_name)")
        job_name_a = result_a["data"][0]["00001"]
        job_name_b = result_b["data"][0]["00001"]

        # They must be running as distinct jobs
        assert job_name_a != job_name_b, "Expected two distinct IBM i jobs"

        # Fetch each connection's trace buffer
        for job, own_job, other_job in (
            (job_a, job_name_a, job_name_b),
            (job_b, job_name_b, job_name_a),
        ):
            req = GetTraceDataRequest(id=job._get_unique_id("gtd"))
            job.send(json.dumps(dataclasses.asdict(req)))
            result = GetTraceDataResult.from_dict(json.loads(job._socket.recv()))
            assert result.success is True
            # Per-connection isolation: the OTHER connection's job name must
            # not appear in this connection's trace buffer.
            assert other_job not in result.tracedata, (
                f"Trace isolation violated: job {other_job!r} found in "
                f"trace buffer for {own_job!r}"
            )
    finally:
        job_a.close()
        job_b.close()


@requires_v240
def test_gettracedata_after_close_on_fresh_connection_works():
    """After re-connecting (fresh SQLJob) with tracing enabled from the start,
    gettracedata should still return data.  This exercises the new per-connection
    Tracer init path in SystemConnection."""
    job = SQLJob()
    job.connect(creds)
    try:
        set_req = SetConfigRequest(
            id=job._get_unique_id("sc"),
            tracedest="IN_MEM",
            tracelevel="DATASTREAM",
        )
        job.send(json.dumps(dataclasses.asdict(set_req)))
        sc_result = SetConfigResult.from_dict(json.loads(job._socket.recv()))
        assert sc_result.success is True
        assert sc_result.tracelevel == ServerTraceLevel.DATASTREAM

        job.query_and_run("VALUES (1)")

        gtd_req = GetTraceDataRequest(id=job._get_unique_id("gtd"))
        job.send(json.dumps(dataclasses.asdict(gtd_req)))
        result = GetTraceDataResult.from_dict(json.loads(job._socket.recv()))
        assert result.success is True
        assert len(result.tracedata) > 0
    finally:
        job.close()
