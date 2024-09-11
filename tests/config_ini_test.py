import os

import pytest

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.data_types import DaemonServer, QueryOptions

# Fetch environment variables
server = os.getenv("VITE_SERVER")
user = os.getenv("VITE_DB_USER")
password = os.getenv("VITE_DB_PASS")
port = os.getenv("VITE_DB_PORT")

# Check if environment variables are set
if not server or not user or not password:
    raise ValueError("One or more environment variables are missing.")


creds = DaemonServer(
    host=server,
    port=port,
    user=user,
    password=password,
    ignoreUnauthorized=True,
)


def test_simple():
    job = SQLJob()
    _ = job.connect("~/.mapepire-test.ini", section="ossbuild")
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=5)
    job.close()
    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True


def test_simple_oss74dev():
    job = SQLJob()
    _ = job.connect("~/.mapepire-test.ini", section="oss74dev")
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=5)
    job.close()
    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True


def test_simple_cm():
    with SQLJob("~/.mapepire-test.ini", section="ossbuild") as job:
        with job.query("select * from sample.employee") as query:
            result = query.run(rows_to_fetch=5)
    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True
