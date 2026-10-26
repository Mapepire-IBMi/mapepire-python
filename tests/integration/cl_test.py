import dataclasses
import json
import os
import re

import pytest  # type: ignore

from mapepire_python import QueryOptions, SQLJob
from mapepire_python.data_types import GetVersionRequest, VersionCheckResult

from .test_setup import *


@pytest.fixture(scope="module")
def requires_v2_5():
    job = SQLJob()
    _ = job.connect(creds)
    job.send(json.dumps(dataclasses.asdict(GetVersionRequest(id="getversion"))))
    version = VersionCheckResult.from_dict(json.loads(job._socket.recv())).version  # type: ignore
    job.close()
    if tuple(int(n) for n in re.findall(r"\d+", version)[:3]) < (2, 5, 0):
        pytest.skip(f"requires Mapepire Server v2.5.0+, server is {version}")


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


@pytest.mark.usefixtures("requires_v2_5")
def test_run_cl_command_successfully():
    job = SQLJob()
    _ = job.connect(creds)
    result_cl = job.rcclcommand("CHKOBJ OBJ(QSYS) OBJTYPE(*LIB)")
    result_sql = job.clcommand("CHKOBJ OBJ(QSYS) OBJTYPE(*LIB)")
    job.close()
    for result in (result_cl, result_sql):
        assert result.success is True
        assert len(result.data) == 0


@pytest.mark.usefixtures("requires_v2_5")
def test_invalid_cl_command():
    job = SQLJob()
    _ = job.connect(creds)
    result_cl = job.rcclcommand("SNDMSG")
    result_sql = job.clcommand("SNDMSG")
    job.close()
    for result in (result_cl, result_sql):
        assert len(result.data) >= 2
        assert result.success is False
    assert result_sql.sql_rc == -443
    assert result_sql.sql_state == "38501"
