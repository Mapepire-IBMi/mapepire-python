"""Unit tests for the clcommand()/rcclcommand() job methods."""

import json

import pytest

from mapepire_python.client.async_sql_job import AsyncSQLJob


@pytest.mark.parametrize("method, request_type", [("clcommand", "cl"), ("rcclcommand", "rccl")])
def test_sql_job(mock_sql_job, method, request_type):
    job, socket = mock_sql_job
    socket.add_response(json.dumps({"id": "1", "success": False, "data": [{"id": "CPF0001"}]}))
    result = getattr(job, method)("SNDMSG")
    assert json.loads(socket.sent[0])["type"] == request_type
    assert result.success is False
    assert result.data[0].id == "CPF0001"


@pytest.mark.asyncio
@pytest.mark.parametrize("method, request_type", [("clcommand", "cl"), ("rcclcommand", "rccl")])
async def test_async_job(method, request_type):
    job = AsyncSQLJob()
    sent = []

    async def send(content):
        sent.append(json.loads(content))
        return {"id": "1", "success": True, "data": []}

    job.send = send  # type: ignore[method-assign]
    result = await getattr(job, method)("WRKACTJOB")
    assert sent[0]["type"] == request_type
    assert result.success is True
