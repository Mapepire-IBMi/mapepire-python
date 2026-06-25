"""Regression tests for context-manager connection behavior.

PR #111 removed the ``if self.creds:`` guard from ``SQLJob.__enter__`` and
``AsyncSQLJob.__aenter__``. Without it, ``with SQLJob() as job:`` (no
credentials) eagerly calls ``connect(None)`` -> ``DaemonServer.from_env()`` and
raises ``ValueError`` when no ``MAPEPIRE_*`` environment variables are set,
breaking the documented "connect later" pattern::

    with SQLJob() as job:
        job.connect(creds)

These tests lock in the restored behavior: entering a context manager without
credentials defers the connection instead of raising. They are
server-independent and run in CI without a live Mapepire server.
"""

import pytest

from mapepire_python.client.async_sql_job import AsyncSQLJob
from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import JobStatus


@pytest.fixture(autouse=True)
def _clear_mapepire_env(monkeypatch):
    # Exercise the no-credentials path: from_env() must have nothing to read.
    for var in ("MAPEPIRE_HOST", "MAPEPIRE_USER", "MAPEPIRE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)


def test_enter_without_creds_defers_connection():
    with SQLJob() as job:
        assert job.get_status() == JobStatus.NotStarted


@pytest.mark.asyncio
async def test_aenter_without_creds_defers_connection():
    async with AsyncSQLJob() as job:
        assert job.get_status() == JobStatus.NotStarted
