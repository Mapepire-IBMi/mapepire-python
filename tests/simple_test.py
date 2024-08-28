import os
import re

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.types import DaemonServer

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


def parse_sql_rc(message):
    match = re.search(r"'sql_rc': (-?\d+)", message)
    if match:
        return int(match.group(1))
    else:
        return None


def test_simple():
    job = SQLJob()
    _ = job.connect(creds)
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=5)
    assert result["success"]
    job.close()


def test_query_and_run():
    job = SQLJob()
    _ = job.connect(creds)
    result = job.query_and_run("select * from sample.employee", rows_to_fetch=5)
    assert result["success"]
    job.close()


def test_paging():
    job = SQLJob()
    _ = job.connect(creds)
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=5)
    while True:
        assert result["data"] is not None and len(result["data"]) > 0
        print(result)
        if result["is_done"]:
            break

        result = query.fetch_more(rows_to_fetch=5)

    job.close()


def test_error():
    job = SQLJob()
    _ = job.connect(creds)

    query = job.query("select * from thisisnotreal")

    try:
        query.run()
    except Exception as e:
        message = str(e)

        assert parse_sql_rc(message) == -204

    job.close()
