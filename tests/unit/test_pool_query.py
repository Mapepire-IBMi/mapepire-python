"""Unit tests for the PoolQuery async state machine (pool/pool_query.py).

PoolQuery mirrors Query but is async: ``job.send()`` is a coroutine that
returns the already-parsed response dict (there is no separate socket.recv).
A small fake job supplies queued dict responses so no network is involved.

Covers NOT_YET_RUN -> RUN_MORE_DATA_AVAIL -> RUN_DONE -> ERROR via run(),
fetch_more() and close().
"""
import json

import pytest

from mapepire_python import QueryOptions
from mapepire_python.client.query import QueryState
from mapepire_python.pool.pool_query import PoolQuery


class _FakeAsyncJob:
    """Minimal async job satisfying PoolQuery's _SQLJobProtocol.

    ``responses`` is a list of dicts returned, in order, by send().
    """

    def __init__(self, responses=None):
        self._responses = list(responses or [])
        self.sent = []
        self.socket = object()  # truthy -> "connected"
        self._counter = 0

    def _get_unique_id(self, prefix: str = "id") -> str:
        self._counter += 1
        return f"{prefix}{self._counter}"

    async def send(self, content: str):
        self.sent.append(content)
        if not self._responses:
            raise RuntimeError("_FakeAsyncJob: no more responses queued")
        return self._responses.pop(0)


def _job(*responses_json):
    """Build a fake job from JSON strings (the conftest factory outputs)."""
    return _FakeAsyncJob([json.loads(r) for r in responses_json])


# ---------------------------------------------------------------------------
# Initial state
# ---------------------------------------------------------------------------

class TestInitialState:
    def test_default_state_is_not_yet_run(self):
        query = PoolQuery(_FakeAsyncJob(), "SELECT 1", QueryOptions())
        assert query.state == QueryState.NOT_YET_RUN

    def test_no_params_means_not_prepared(self):
        query = PoolQuery(_FakeAsyncJob(), "SELECT 1", QueryOptions())
        assert query.is_prepared is False

    def test_params_marks_prepared(self):
        query = PoolQuery(_FakeAsyncJob(), "SELECT ?", QueryOptions(parameters=[1]))
        assert query.is_prepared is True


# ---------------------------------------------------------------------------
# run() transitions
# ---------------------------------------------------------------------------

class TestRunTransitions:
    @pytest.mark.asyncio
    async def test_success_is_done_transitions_to_run_done(self, make_query_result):
        job = _job(make_query_result(is_done=True, has_results=True))
        query = PoolQuery(job, "SELECT 1", QueryOptions())
        await query.run()
        assert query.state == QueryState.RUN_DONE

    @pytest.mark.asyncio
    async def test_success_not_done_transitions_to_run_more_data_avail(self, make_query_result):
        job = _job(make_query_result(is_done=False, has_results=True))
        query = PoolQuery(job, "SELECT * FROM T", QueryOptions())
        await query.run()
        assert query.state == QueryState.RUN_MORE_DATA_AVAIL

    @pytest.mark.asyncio
    async def test_failure_transitions_to_error_and_raises(self, make_query_result):
        job = _job(make_query_result(success=False, is_done=False, error="bad sql"))
        query = PoolQuery(job, "SELECT bogus", QueryOptions())
        with pytest.raises(Exception):
            await query.run()
        assert query.state == QueryState.ERROR

    @pytest.mark.asyncio
    async def test_returns_query_result(self, make_query_result):
        job = _job(make_query_result(is_done=True, has_results=True))
        query = PoolQuery(job, "SELECT 1", QueryOptions())
        result = await query.run()
        assert result is not None
        assert result.success is True

    @pytest.mark.asyncio
    async def test_correlation_id_set_from_response(self, make_query_result):
        job = _job(make_query_result(is_done=False, id="corr-xyz"))
        query = PoolQuery(job, "SELECT * FROM T", QueryOptions())
        await query.run()
        assert query._correlation_id == "corr-xyz"

    @pytest.mark.asyncio
    async def test_raises_when_already_run_more_data_avail(self, make_query_result):
        job = _job(make_query_result(is_done=False, has_results=True))
        query = PoolQuery(job, "SELECT * FROM T", QueryOptions())
        await query.run()
        with pytest.raises(Exception, match="already been run"):
            await query.run()

    @pytest.mark.asyncio
    async def test_raises_when_already_run_done(self, make_query_result):
        job = _job(make_query_result(is_done=True))
        query = PoolQuery(job, "SELECT 1", QueryOptions())
        await query.run()
        with pytest.raises(Exception, match="already been fully run"):
            await query.run()

    @pytest.mark.asyncio
    async def test_cl_command_failure_does_not_raise(self, make_query_result):
        job = _job(make_query_result(success=False, is_done=True))
        query = PoolQuery(job, "WRKACTJOB", QueryOptions(isClCommand=True))
        result = await query.run()
        assert result is not None
        assert query.state == QueryState.RUN_DONE


# ---------------------------------------------------------------------------
# fetch_more() transitions
# ---------------------------------------------------------------------------

class TestFetchMoreTransitions:
    @pytest.mark.asyncio
    async def test_raises_when_not_yet_run(self):
        query = PoolQuery(_FakeAsyncJob(), "SELECT 1", QueryOptions())
        with pytest.raises(Exception, match="not been run"):
            await query.fetch_more()

    @pytest.mark.asyncio
    async def test_raises_when_run_done(self, make_query_result):
        job = _job(make_query_result(is_done=True))
        query = PoolQuery(job, "SELECT 1", QueryOptions())
        await query.run()
        with pytest.raises(Exception, match="already been fully run"):
            await query.fetch_more()

    @pytest.mark.asyncio
    async def test_transitions_to_run_done_when_is_done(self, make_query_result, make_sql_more_response):
        job = _job(
            make_query_result(is_done=False, has_results=True, id="q1"),
            make_sql_more_response(is_done=True, id="q1"),
        )
        query = PoolQuery(job, "SELECT * FROM T", QueryOptions())
        await query.run()
        await query.fetch_more()
        assert query.state == QueryState.RUN_DONE

    @pytest.mark.asyncio
    async def test_stays_more_data_avail_when_not_done(self, make_query_result, make_sql_more_response):
        job = _job(
            make_query_result(is_done=False, has_results=True, id="q1"),
            make_sql_more_response(is_done=False, id="q1"),
        )
        query = PoolQuery(job, "SELECT * FROM T", QueryOptions())
        await query.run()
        await query.fetch_more()
        assert query.state == QueryState.RUN_MORE_DATA_AVAIL

    @pytest.mark.asyncio
    async def test_failure_sets_error_state(self, make_query_result, make_sql_more_response):
        job = _job(
            make_query_result(is_done=False, has_results=True, id="q1"),
            make_sql_more_response(success=False, id="q1", error="connection lost"),
        )
        query = PoolQuery(job, "SELECT * FROM T", QueryOptions())
        await query.run()
        with pytest.raises(Exception):
            await query.fetch_more()
        assert query.state == QueryState.ERROR


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------

class TestClose:
    @pytest.mark.asyncio
    async def test_close_when_not_run_sets_done_without_send(self):
        job = _FakeAsyncJob()
        query = PoolQuery(job, "SELECT 1", QueryOptions())
        await query.close()
        assert query.state == QueryState.RUN_DONE
        assert job.sent == []  # no correlation_id -> no network call

    @pytest.mark.asyncio
    async def test_close_sends_sqlclose_when_more_data_available(
        self, make_query_result, make_close_response
    ):
        job = _job(
            make_query_result(is_done=False, has_results=True, id="q1"),
            make_close_response(),
        )
        query = PoolQuery(job, "SELECT * FROM T", QueryOptions())
        await query.run()
        assert query.state == QueryState.RUN_MORE_DATA_AVAIL
        await query.close()
        assert query.state == QueryState.RUN_DONE
        assert len(job.sent) == 2  # run + sqlclose

    @pytest.mark.asyncio
    async def test_close_when_run_done_is_noop(self, make_query_result):
        job = _job(make_query_result(is_done=True))
        query = PoolQuery(job, "SELECT 1", QueryOptions())
        await query.run()
        await query.close()
        assert query.state == QueryState.RUN_DONE
        assert len(job.sent) == 1  # only the run; close sent nothing

    @pytest.mark.asyncio
    async def test_close_raises_when_not_connected(self):
        job = _FakeAsyncJob()
        job.socket = None  # not connected
        query = PoolQuery(job, "SELECT 1", QueryOptions())
        with pytest.raises(Exception, match="not connected"):
            await query.close()
