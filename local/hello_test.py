import concurrent.futures
import re

from python_wsdb.client.sql_job import SQLJob
from python_wsdb.ssl import get_certificate
from python_wsdb.types import DaemonServer


def parse_sql_rc(message):
    match = re.search(r"'sql_rc': (-?\d+)", message)
    if match:
        return int(match.group(1))
    else:
        return None


creds = DaemonServer(host="localhost", port=8085, user="ashedivy", password="",)


def test_get_cert():
    ca = get_certificate(creds)
    assert ca is not None


def test_simple():
    ca = get_certificate(creds)
    # creds.ca = ca.raw if ca else None
    creds.ca = ca

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


def test_multiple_statements():
    job = SQLJob()
    _ = job.connect(creds)

    resA = job.query("select * from sample.department").run()
    assert resA["is_done"] is True

    resB = job.query("select * from sample.employee").run()
    assert resB["is_done"] is True

    job.close()


def test_multiple_parallel():
    job = SQLJob()
    _ = job.connect(creds)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        f1 = executor.submit(job.query_and_run("select * from sample.department"))
        f2 = executor.submit(job.query_and_run("select * from sample.employee"))

        concurrent.futures.wait([f1, f2], return_when=concurrent.futures.ALL_COMPLETED)
