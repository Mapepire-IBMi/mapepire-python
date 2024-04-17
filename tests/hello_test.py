import asyncio
from python_sc.client.sql_job import SQLJob
from python_sc.tls import get_certificate
from python_sc.types import DaemonServer

creds = DaemonServer(
    host="localhost",
    port=8085,
    user="ashedivy",
    password="ashedivy1234567",
    ignoreUnauthorized=False
)


def test_simple():
    # ca = asyncio.run(get_certificate(creds))
    # creds.ca = ca.raw if ca else None
    
    job = SQLJob()
    res = job.connect(creds)
    query = job.query('select * from sample.employee')
    result = query.run(rows_to_fetch=5)
    print(result)
    
def test_query_and_run():
    job = SQLJob()
    res = job.connect(creds)
    result = job.query_and_run('select * from sample.employee', rows_to_fetch=5)
    print(result)
    
    