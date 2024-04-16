import asyncio
from python_sc.sql_job import SQLJob
from python_sc.sql_runner import SQLJobRunner
from python_sc.tls import get_certificate
from python_sc.types import DaemonServer

creds = DaemonServer(
    host="localhost",
    port=8085,
    user="ashedivy",
    password="",
    ignoreUnauthorized=False
)


def test_channel_connect():
    # ca = asyncio.run(get_certificate(creds))
    # creds.ca = ca.raw if ca else None
    
    job = SQLJob()
    job.connect(creds)