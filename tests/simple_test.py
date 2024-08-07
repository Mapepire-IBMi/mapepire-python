import os

from python_wsdb.client.sql_job import SQLJob
from python_wsdb.types import DaemonServer

# Fetch environment variables
server = os.getenv("VITE_SERVER")
user = os.getenv("VITE_DB_USER")
password = os.getenv("VITE_DB_PASS")

# Check if environment variables are set
if not server or not user or not password:
    raise ValueError("One or more environment variables are missing.")

creds = DaemonServer(
    host=server,
    port=8085,
    user=user,
    password=password,
    ignoreUnauthorized=True,
)

def test_simple():
    job = SQLJob()
    _ = job.connect(creds)
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=5)
    assert result["success"]
    job.close()
