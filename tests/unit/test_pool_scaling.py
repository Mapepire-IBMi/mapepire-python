"""Unit tests for Pool scaling and job-selection logic.

Tests run without a live server by injecting mock PoolJob objects directly
into ``pool.jobs`` and by patching ``pool._add_job`` where needed.

This file provides unit-level coverage for the four commented-out integration
tests in tests/pooling_test.py that were blocked by JobStatus-tracking
difficulties:
  - test_pop_job_with_pool_ignore
  - test_pool_with_no_space_no_ready_job_doesnt_increase_pool_size
  - test_pool_with_space_but_no_ready_job_adds_job_to_pool
  - test_freeist_job_is_returned
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mapepire_python import DaemonServer, JobStatus
from mapepire_python.pool.pool_client import Pool, PoolAddOptions, PoolOptions

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pool(max_size: int = 5, starting_size: int = 3) -> Pool:
    creds = DaemonServer(
        host="test.host", user="user", password="pass", port=8076, ignoreUnauthorized=True
    )
    return Pool(PoolOptions(creds=creds, max_size=max_size, starting_size=starting_size))


def _mock_job(status: JobStatus = JobStatus.Ready, running_count: int = 0) -> MagicMock:
    job = MagicMock()
    job.get_status.return_value = status
    job.get_running_count.return_value = running_count
    job.get_unique_id.return_value = f"sqljob{id(job)}"
    job.close = AsyncMock()
    return job


# ---------------------------------------------------------------------------
# Pool.__init__ / Pool.init validation
# ---------------------------------------------------------------------------

class TestPoolValidation:
    @pytest.mark.asyncio
    async def test_init_raises_when_max_size_is_zero(self):
        pool = _make_pool(max_size=0, starting_size=1)
        with pytest.raises(ValueError, match="Max size must be greater than 0"):
            await pool.init()

    @pytest.mark.asyncio
    async def test_init_raises_when_starting_size_is_zero(self):
        pool = _make_pool(max_size=5, starting_size=0)
        with pytest.raises(ValueError, match="Starting size must be greater than 0"):
            await pool.init()

    @pytest.mark.asyncio
    async def test_init_raises_when_starting_size_exceeds_max_size(self):
        pool = _make_pool(max_size=2, starting_size=5)
        with pytest.raises(ValueError, match="Max size must be greater than or equal to starting size"):
            await pool.init()

    @pytest.mark.asyncio
    async def test_init_calls_add_job_starting_size_times(self):
        pool = _make_pool(max_size=5, starting_size=3)
        pool._add_job = AsyncMock()
        await pool.init()
        assert pool._add_job.call_count == 3


# ---------------------------------------------------------------------------
# Pool.has_space
# ---------------------------------------------------------------------------

class TestHasSpace:
    def test_returns_true_when_active_jobs_below_max(self):
        pool = _make_pool(max_size=5, starting_size=3)
        pool.jobs = [_mock_job(JobStatus.Ready) for _ in range(3)]
        assert pool.has_space() is True

    def test_returns_false_when_active_jobs_equal_max(self):
        pool = _make_pool(max_size=3, starting_size=3)
        pool.jobs = [_mock_job(JobStatus.Ready) for _ in range(3)]
        assert pool.has_space() is False

    def test_ended_jobs_are_not_counted(self):
        pool = _make_pool(max_size=3, starting_size=3)
        pool.jobs = [
            _mock_job(JobStatus.Ready),
            _mock_job(JobStatus.Ended),
            _mock_job(JobStatus.Ended),
        ]
        # Only 1 active job vs max 3 → has space
        assert pool.has_space() is True

    def test_not_started_jobs_are_not_counted(self):
        pool = _make_pool(max_size=3, starting_size=3)
        pool.jobs = [
            _mock_job(JobStatus.Ready),
            _mock_job(JobStatus.NotStarted),
        ]
        assert pool.has_space() is True

    def test_busy_jobs_count_toward_limit(self):
        pool = _make_pool(max_size=2, starting_size=2)
        pool.jobs = [
            _mock_job(JobStatus.Busy),
            _mock_job(JobStatus.Busy),
        ]
        assert pool.has_space() is False


# ---------------------------------------------------------------------------
# Pool.get_active_job_count
# ---------------------------------------------------------------------------

class TestGetActiveJobCount:
    def test_counts_ready_and_busy_jobs(self):
        pool = _make_pool()
        pool.jobs = [
            _mock_job(JobStatus.Ready),
            _mock_job(JobStatus.Busy),
            _mock_job(JobStatus.Ended),
            _mock_job(JobStatus.NotStarted),
        ]
        assert pool.get_active_job_count() == 2

    def test_zero_when_all_jobs_ended(self):
        pool = _make_pool()
        pool.jobs = [_mock_job(JobStatus.Ended) for _ in range(3)]
        assert pool.get_active_job_count() == 0

    def test_empty_pool_returns_zero(self):
        pool = _make_pool()
        assert pool.get_active_job_count() == 0


# ---------------------------------------------------------------------------
# Pool.cleanup
# ---------------------------------------------------------------------------

class TestCleanup:
    def test_removes_ended_jobs(self):
        pool = _make_pool()
        ready = _mock_job(JobStatus.Ready)
        pool.jobs = [_mock_job(JobStatus.Ended), ready, _mock_job(JobStatus.Ended)]
        pool.cleanup()
        assert pool.jobs == [ready]

    def test_removes_not_started_jobs(self):
        pool = _make_pool()
        ready = _mock_job(JobStatus.Ready)
        pool.jobs = [_mock_job(JobStatus.NotStarted), ready]
        pool.cleanup()
        assert pool.jobs == [ready]

    def test_preserves_busy_and_ready_jobs(self):
        pool = _make_pool()
        jobs = [_mock_job(JobStatus.Ready), _mock_job(JobStatus.Busy)]
        pool.jobs = list(jobs)
        pool.cleanup()
        assert pool.jobs == jobs


# ---------------------------------------------------------------------------
# Pool._get_ready_job / Pool._get_ready_job_idx
# ---------------------------------------------------------------------------

class TestGetReadyJob:
    def test_returns_first_ready_job(self):
        pool = _make_pool()
        busy = _mock_job(JobStatus.Busy)
        ready1 = _mock_job(JobStatus.Ready)
        ready2 = _mock_job(JobStatus.Ready)
        pool.jobs = [busy, ready1, ready2]
        assert pool._get_ready_job() is ready1

    def test_returns_none_when_no_ready_job(self):
        pool = _make_pool()
        pool.jobs = [_mock_job(JobStatus.Busy), _mock_job(JobStatus.Ended)]
        assert pool._get_ready_job() is None

    def test_idx_returns_index_of_first_ready_job(self):
        pool = _make_pool()
        pool.jobs = [_mock_job(JobStatus.Busy), _mock_job(JobStatus.Ready)]
        assert pool._get_ready_job_idx() == 1

    def test_idx_returns_negative_one_when_no_ready_job(self):
        pool = _make_pool()
        pool.jobs = [_mock_job(JobStatus.Busy), _mock_job(JobStatus.Ended)]
        assert pool._get_ready_job_idx() == -1


# ---------------------------------------------------------------------------
# Pool.get_job  (covers test_freeist_job_is_returned)
# ---------------------------------------------------------------------------

class TestGetJob:
    @pytest.mark.asyncio
    async def test_returns_ready_job_when_available(self):
        # Load-aware selection (PR #115) picks the least-loaded usable job.
        # The busy job has in-flight work, so the idle Ready job wins.
        pool = _make_pool()
        busy = _mock_job(JobStatus.Busy, running_count=3)
        ready = _mock_job(JobStatus.Ready, running_count=0)
        pool.jobs = [busy, ready]
        result = await pool.get_job()
        assert result is ready

    @pytest.mark.asyncio
    async def test_returns_least_loaded_busy_job_when_no_ready_job(self):
        """Equivalent unit coverage for test_freeist_job_is_returned.

        With max_size == len(jobs) there is no space, so _add_job is not
        called regardless of running_count.
        """
        pool = _make_pool(max_size=2, starting_size=2)
        heavy = _mock_job(JobStatus.Busy, running_count=8)
        light = _mock_job(JobStatus.Busy, running_count=2)
        pool.jobs = [heavy, light]
        result = await pool.get_job()
        assert result is light

    @pytest.mark.asyncio
    async def test_adds_job_when_pool_has_space_and_all_jobs_are_busy_with_high_load(self):
        """Equivalent unit coverage for test_pool_with_space_but_no_ready_job_adds_job_to_pool."""
        pool = _make_pool(max_size=2, starting_size=1)
        heavy = _mock_job(JobStatus.Busy, running_count=5)  # > 2
        pool.jobs = [heavy]

        new_job = _mock_job(JobStatus.Ready)
        pool._add_job = AsyncMock(return_value=new_job)

        await pool.get_job()
        pool._add_job.assert_called_once()

    @pytest.mark.asyncio
    async def test_does_not_add_job_when_pool_is_full(self):
        """Equivalent unit coverage for
        test_pool_with_no_space_no_ready_job_doesnt_increase_pool_size."""
        pool = _make_pool(max_size=1, starting_size=1)
        busy = _mock_job(JobStatus.Busy, running_count=10)
        pool.jobs = [busy]

        pool._add_job = AsyncMock()
        await pool.get_job()
        pool._add_job.assert_not_called()


# ---------------------------------------------------------------------------
# Pool.pop_job  (covers test_pop_job_with_pool_ignore)
# ---------------------------------------------------------------------------

class TestPopJob:
    @pytest.mark.asyncio
    async def test_removes_and_returns_first_ready_job(self):
        pool = _make_pool(max_size=5, starting_size=5)
        busy = _mock_job(JobStatus.Busy)
        ready = _mock_job(JobStatus.Ready)
        pool.jobs = [busy, ready]

        result = await pool.pop_job()

        assert result is ready
        assert ready not in pool.jobs
        assert busy in pool.jobs

    @pytest.mark.asyncio
    async def test_creates_pool_ignore_job_when_no_ready_jobs(self):
        """Equivalent unit coverage for test_pop_job_with_pool_ignore.

        When no ready job exists pop_job() creates a transient job with
        pool_ignore=True so the pool size is not permanently increased.
        """
        pool = _make_pool(max_size=1, starting_size=1)
        busy = _mock_job(JobStatus.Busy)
        pool.jobs = [busy]

        transient = _mock_job(JobStatus.Ready)
        add_job_calls = []

        async def _fake_add_job(opts=None):
            add_job_calls.append(opts)
            return transient

        pool._add_job = _fake_add_job

        result = await pool.pop_job()

        assert result is transient
        assert len(add_job_calls) == 1
        # pool_ignore must be True so the transient job isn't added to pool.jobs
        assert add_job_calls[0].pool_ignore is True
        # busy job must still be in the pool
        assert busy in pool.jobs

    @pytest.mark.asyncio
    async def test_pool_size_not_permanently_increased_by_pop_with_pool_ignore(self):
        """After pop_job uses pool_ignore, pool.jobs length stays the same."""
        pool = _make_pool(max_size=1, starting_size=1)
        busy = _mock_job(JobStatus.Busy)
        pool.jobs = [busy]
        original_size = len(pool.jobs)

        transient = _mock_job(JobStatus.Ready)
        pool._add_job = AsyncMock(return_value=transient)

        await pool.pop_job()
        assert len(pool.jobs) == original_size


# ---------------------------------------------------------------------------
# Pool.wait_for_job
# ---------------------------------------------------------------------------

class TestWaitForJob:
    @pytest.mark.asyncio
    async def test_returns_ready_job_when_available(self):
        pool = _make_pool()
        ready = _mock_job(JobStatus.Ready)
        pool.jobs = [ready]
        result = await pool.wait_for_job()
        assert result is ready

    @pytest.mark.asyncio
    async def test_adds_job_when_no_ready_job_and_has_space(self):
        pool = _make_pool(max_size=3, starting_size=1)
        pool.jobs = [_mock_job(JobStatus.Busy)]
        new_job = _mock_job(JobStatus.Ready)
        pool._add_job = AsyncMock(return_value=new_job)

        result = await pool.wait_for_job()
        assert result is new_job

    @pytest.mark.asyncio
    async def test_use_new_job_flag_bypasses_space_check(self):
        pool = _make_pool(max_size=1, starting_size=1)
        pool.jobs = [_mock_job(JobStatus.Busy)]
        new_job = _mock_job(JobStatus.Ready)
        pool._add_job = AsyncMock(return_value=new_job)

        result = await pool.wait_for_job(use_new_job=True)
        assert result is new_job


# ---------------------------------------------------------------------------
# Pool.end
# ---------------------------------------------------------------------------

class TestPoolEnd:
    @pytest.mark.asyncio
    async def test_end_closes_all_jobs(self):
        pool = _make_pool()
        jobs = [_mock_job() for _ in range(3)]
        pool.jobs = jobs
        await pool.end()
        for job in jobs:
            job.close.assert_called_once()
