import os

from python_wsdb.client.sql_job import SQLJob
from python_wsdb.types import DaemonServer

creds = DaemonServer(
    host=os.getenv("VITE_SERVER"),
    port=8085,
    user=os.getenv("VITE_DB_USER"),
    password=os.getenv("VITE_DB_PASS"),
    ignoreUnauthorized=True,
)


def test_simple():
    job = SQLJob()
    _ = job.connect(creds)
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=5)
    assert result["success"]
    job.close()
