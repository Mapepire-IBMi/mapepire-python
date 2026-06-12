"""Unit tests for the Query state machine (mapepire_python.client.query).

All tests use MockSocket injected into a pre-connected SQLJob so that no
network I/O occurs.  The response factories come from tests/conftest.py.
"""
import pytest

from mapepire_python import QueryOptions
from mapepire_python.client.query import Query, QueryState

# ---------------------------------------------------------------------------
# Initial state
# ---------------------------------------------------------------------------

class TestQueryInitialState:
    def test_default_state_is_not_yet_run(self, mock_sql_job):
        job, _ = mock_sql_job
        query = Query(job, "SELECT 1 FROM SYSIBM.SYSDUMMY1", QueryOptions())
        assert query.state == QueryState.NOT_YET_RUN

    def test_no_params_means_not_prepared(self, mock_sql_job):
        job, _ = mock_sql_job
        query = Query(job, "SELECT 1", QueryOptions())
        assert query.is_prepared is False

    def test_params_in_opts_marks_prepared(self, mock_sql_job):
        job, _ = mock_sql_job
        query = Query(job, "SELECT ? FROM SYSIBM.SYSDUMMY1", QueryOptions(parameters=[42]))
        assert query.is_prepared is True

    def test_correlation_id_is_none_initially(self, mock_sql_job):
        job, _ = mock_sql_job
        query = Query(job, "SELECT 1", QueryOptions())
        assert query._correlation_id is None

    def test_default_rows_to_fetch_is_100(self, mock_sql_job):
        job, _ = mock_sql_job
        query = Query(job, "SELECT 1", QueryOptions())
        assert query._rows_to_fetch == 100


# ---------------------------------------------------------------------------
# prepare_sql_execute state transitions
# ---------------------------------------------------------------------------

class TestPrepareSqlExecuteTransitions:
    def test_is_done_false_transitions_to_run_more_data_avail(
        self, mock_sql_job, make_query_result
    ):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=False, has_results=True))
        query = Query(job, "SELECT * FROM TABLE", QueryOptions())
        query.prepare_sql_execute()
        assert query.state == QueryState.RUN_MORE_DATA_AVAIL

    def test_is_done_true_transitions_to_run_done(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=True, has_results=True))
        query = Query(job, "SELECT 1", QueryOptions())
        query.prepare_sql_execute()
        assert query.state == QueryState.RUN_DONE

    def test_correlation_id_set_from_response_id(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=False, id="resp-abc-123"))
        query = Query(job, "SELECT * FROM TABLE", QueryOptions())
        query.prepare_sql_execute()
        assert query._correlation_id == "resp-abc-123"

    def test_raises_when_already_run_done(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=True))
        query = Query(job, "SELECT 1", QueryOptions())
        query.prepare_sql_execute()
        # Second call should raise immediately (no socket needed)
        with pytest.raises(Exception, match="already been fully run"):
            query.prepare_sql_execute()

    def test_failed_response_transitions_to_error_state(
        self, mock_sql_job, make_query_result
    ):
        job, socket = mock_sql_job
        socket.add_response(
            make_query_result(success=False, is_done=False, error="Column NOT_A_COL not found")
        )
        query = Query(job, "SELECT NOT_A_COL FROM TABLE", QueryOptions())
        with pytest.raises(RuntimeError):
            query.prepare_sql_execute()
        assert query.state == QueryState.ERROR

    def test_failed_response_with_no_error_fields_uses_fallback_message(
        self, mock_sql_job, make_query_result
    ):
        job, socket = mock_sql_job
        # success=False but error/sql_rc/sql_state all empty/zero
        socket.add_response(make_query_result(success=False, is_done=False))
        query = Query(job, "SELECT 1", QueryOptions())
        with pytest.raises(RuntimeError) as exc_info:
            query.prepare_sql_execute()
        assert "unknown reason" in str(exc_info.value)

    def test_returns_query_result_on_success(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=True, has_results=True))
        query = Query(job, "SELECT 1", QueryOptions())
        result = query.prepare_sql_execute()
        assert result is not None
        assert result.success is True


# ---------------------------------------------------------------------------
# run() state transitions
# ---------------------------------------------------------------------------

class TestRunTransitions:
    def test_success_is_done_transitions_to_run_done(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=True, has_results=True))
        query = Query(job, "SELECT 1", QueryOptions())
        query.run()
        assert query.state == QueryState.RUN_DONE

    def test_success_not_done_transitions_to_run_more_data_avail(
        self, mock_sql_job, make_query_result
    ):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=False, has_results=True))
        query = Query(job, "SELECT * FROM TABLE", QueryOptions())
        query.run()
        assert query.state == QueryState.RUN_MORE_DATA_AVAIL

    def test_failure_transitions_to_error(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(success=False, is_done=False, error="bad sql"))
        query = Query(job, "SELECT bogus", QueryOptions())
        with pytest.raises(RuntimeError):
            query.run()
        assert query.state == QueryState.ERROR

    def test_returns_query_result_on_success(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=True, has_results=True))
        query = Query(job, "SELECT 1", QueryOptions())
        result = query.run()
        assert result is not None
        assert result.success is True

    def test_raises_when_already_run_more_data_avail(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=False, has_results=True))
        query = Query(job, "SELECT * FROM TABLE", QueryOptions())
        query.run()
        # Second run() while more data is available raises immediately.
        with pytest.raises(Exception, match="already been run"):
            query.run()

    def test_raises_when_already_run_done(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=True))
        query = Query(job, "SELECT 1", QueryOptions())
        query.run()
        with pytest.raises(Exception, match="already been fully run"):
            query.run()

    def test_rows_to_fetch_is_recorded(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=True))
        query = Query(job, "SELECT 1", QueryOptions())
        query.run(rows_to_fetch=25)
        assert query._rows_to_fetch == 25

    def test_cl_command_failure_does_not_raise(self, mock_sql_job, make_query_result):
        # The error branch is guarded by `not is_cl_command`, so a failed CL
        # response transitions normally without raising.
        job, socket = mock_sql_job
        socket.add_response(make_query_result(success=False, is_done=True))
        query = Query(job, "WRKACTJOB", QueryOptions(isClCommand=True))
        result = query.run()
        assert result is not None
        assert query.state == QueryState.RUN_DONE


# ---------------------------------------------------------------------------
# fetch_more state transitions
# ---------------------------------------------------------------------------

class TestFetchMoreTransitions:
    def test_raises_when_not_yet_run(self, mock_sql_job):
        job, _ = mock_sql_job
        query = Query(job, "SELECT 1", QueryOptions())
        with pytest.raises(Exception, match="not been run"):
            query.fetch_more()

    def test_raises_when_run_done(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=True))
        query = Query(job, "SELECT 1", QueryOptions())
        query.prepare_sql_execute()
        with pytest.raises(Exception, match="already been fully run"):
            query.fetch_more()

    def test_transitions_to_run_done_when_is_done_true(
        self, mock_sql_job, make_query_result, make_sql_more_response
    ):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=False, has_results=True, id="q1"))
        socket.add_response(make_sql_more_response(is_done=True, id="q1"))
        query = Query(job, "SELECT * FROM TABLE", QueryOptions())
        query.prepare_sql_execute()
        query.fetch_more()
        assert query.state == QueryState.RUN_DONE

    def test_remains_run_more_data_avail_when_is_done_false(
        self, mock_sql_job, make_query_result, make_sql_more_response
    ):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=False, has_results=True, id="q1"))
        socket.add_response(make_sql_more_response(is_done=False, id="q1"))
        query = Query(job, "SELECT * FROM TABLE", QueryOptions())
        query.prepare_sql_execute()
        query.fetch_more()
        assert query.state == QueryState.RUN_MORE_DATA_AVAIL

    def test_failed_fetch_more_sets_error_state(
        self, mock_sql_job, make_query_result, make_sql_more_response
    ):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=False, has_results=True, id="q1"))
        socket.add_response(
            make_sql_more_response(success=False, id="q1", error="Connection lost")
        )
        query = Query(job, "SELECT * FROM TABLE", QueryOptions())
        query.prepare_sql_execute()
        with pytest.raises(RuntimeError):
            query.fetch_more()
        assert query.state == QueryState.ERROR

    def test_rows_to_fetch_updated(
        self, mock_sql_job, make_query_result, make_sql_more_response
    ):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=False, has_results=True, id="q1"))
        socket.add_response(make_sql_more_response(is_done=True, id="q1"))
        query = Query(job, "SELECT * FROM TABLE", QueryOptions())
        query.prepare_sql_execute()
        query.fetch_more(rows_to_fetch=50)
        assert query._rows_to_fetch == 50


# ---------------------------------------------------------------------------
# close() behaviour
# ---------------------------------------------------------------------------

class TestQueryClose:
    def test_close_when_not_yet_run_sets_done(self, mock_sql_job):
        """Query with no correlation_id just marks itself done without network call."""
        job, socket = mock_sql_job
        query = Query(job, "SELECT 1", QueryOptions())
        # No response queued – close() should not call recv()
        query.close()
        assert query.state == QueryState.RUN_DONE

    def test_close_when_run_done_is_noop(self, mock_sql_job, make_query_result):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=True))
        query = Query(job, "SELECT 1", QueryOptions())
        query.prepare_sql_execute()
        assert query.state == QueryState.RUN_DONE
        # close() on a RUN_DONE query with a correlation_id skips the network call
        query.close()
        assert query.state == QueryState.RUN_DONE

    def test_close_sends_sqlclose_when_more_data_available(
        self, mock_sql_job, make_query_result, make_close_response
    ):
        job, socket = mock_sql_job
        socket.add_response(make_query_result(is_done=False, has_results=True, id="q1"))
        socket.add_response(make_close_response())
        query = Query(job, "SELECT * FROM TABLE", QueryOptions())
        query.prepare_sql_execute()
        assert query.state == QueryState.RUN_MORE_DATA_AVAIL
        query.close()
        # One send for prepare_sql_execute + one for sqlclose
        assert len(socket.sent) == 2
        assert query.state == QueryState.RUN_DONE
