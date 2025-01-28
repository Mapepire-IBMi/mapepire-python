import os

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


creds = DaemonServer(host=server, port=port, user=user, password=password)


def test_simple():
    job = SQLJob()
    _ = job.connect(creds)
    query = job.query("select * from sample.employee")
    result = query.run(rows_to_fetch=5)
    job.close()
    assert result["success"] is True
    assert result["is_done"] is False
    assert result["has_results"] is True


def test_cl_succesful():
    job = SQLJob()
    _ = job.connect(creds)
    opts = QueryOptions(isClCommand=True)
    query = job.query("WRKACTJOB", opts=opts)
    result = query.run()
    job.close()
    assert len(result["data"]) >= 1
    assert result["success"] is True


def test_cl_unsuccesful():
    job = SQLJob()
    _ = job.connect(creds)
    opts = QueryOptions(isClCommand=True)
    query = job.query("INVALIDCOMMAND", opts=opts)
    result = query.run()
    job.close()
    assert len(result["data"]) >= 1
    assert result["success"] is False
    assert "[CPF0006] Errors occurred in command." in result["error"]
    assert result["id"] is not None
    assert result["is_done"] is True
    assert result["sql_rc"] == -443
    assert result["sql_state"] == "38501"
