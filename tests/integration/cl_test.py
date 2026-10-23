import os

from mapepire_python import QueryOptions, SQLJob

from .test_setup import *


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
    # whether WRKACTJOB writes job log messages depends on the server job's logging level
    assert isinstance(result["data"], list)
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
    # mapepire-server < 2.5.0 reports the generic CPF0006; 2.5.0+ (mapepire-server#199)
    # reports the underlying escape message, e.g. "[CPF0001] Error found on *N command."
    assert "[CPF0006]" in result["error"] or "[CPF0001]" in result["error"]
    assert result["id"] is not None
    assert result["is_done"] is True
    assert result["sql_rc"] == -443
    assert result["sql_state"] == "38501"
