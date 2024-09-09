import os

from mapepire_python import connect
from mapepire_python.data_types import DaemonServer, QueryOptions

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


def test_pep249():
    conn = connect(creds)
    cur = conn.execute("select * from sample.employee")
    assert cur.rowcount == -1
    res = cur.fetchmany(5)
    cur.close()
    conn.close()
    assert len(res["data"]) == 5


def test_pep249_no_size():
    conn = connect(creds)
    cur = conn.execute("select * from sample.employee")
    res = cur.fetchmany()
    assert len(res["data"]) == 1


def test_pep249_set_array_size():
    conn = connect(creds)
    cur = conn.execute("select * from sample.employee")
    cur.arraysize = 10
    res = cur.fetchmany()
    assert len(res["data"]) == 10


def test_pep249_cm_fetchmany():
    with connect(creds) as connection:
        with connection.execute("select * from sample.employee") as cur:
            res = cur.fetchmany(5)
            assert len(res["data"]) == 5


def test_pep249_cm_fetchall():
    with connect(creds) as connection:
        with connection.execute("select * from sample.employee") as cur:
            res = cur.fetchall()
            assert len(res["data"]) > 5


def test_pep249_cm_fetchone():
    with connect(creds) as connection:
        with connection.execute("select * from sample.employee") as cur:
            res = cur.fetchone()
            print(res["data"])
            assert len(res["data"]) == 1

            res2 = cur.fetchone()
            print(res2["data"])
            assert res["data"] != res2["data"]


def test_pep249_cm_next():
    with connect(creds) as connection:
        with connection.execute("select * from sample.employee") as cur:
            assert next(cur) is not None


def test_pep249_query_queue():
    conn = connect(creds)
    cur = conn.cursor()
    cur.execute("select * from sample.employee")
    cur.execute("select * from sample.department")
    assert len(cur.query_q) == 2
    print("Employee\n")
    row = cur.fetchone()
    while row is not None:
        row = cur.fetchone()

    print("\nDepartment:\n")
    cur.nextset()
    row = cur.fetchone()
    while row is not None:
        row = cur.fetchone()


def test_pep249_query_queue_error():
    conn = connect(creds)
    cur = conn.cursor()
    cur.execute("select * from sample.employee")
    print("Employee\n")
    row = cur.fetchone()
    while row is not None:
        row = cur.fetchone()

    next_set = cur.nextset()
    assert next_set is None


def test_prepare_statement_mult_params():
    conn = connect(creds)
    cur = conn.cursor()
    opts = QueryOptions(parameters=[500, "PRES"])
    cur.execute("select * from sample.employee where bonus > ? and job = ?", opts=opts)
    res = cur.fetchall()
    assert res["success"] == True


def test_prepare_statement_mult_params_seq():
    conn = connect(creds)
    cur = conn.cursor()
    parameters = [500, "PRES"]
    cur.execute("select * from sample.employee where bonus > ? and job = ?", parameters=parameters)
    res = cur.fetchall()
    assert res["success"] == True


def test_pep249_iterate():
    def rows():
        with connect(creds) as conn:
            for row in conn.execute("select * from sample.department"):
                yield row

    cool_rows = rows()
    for row in cool_rows:
        assert row["success"] == True


def test_pep249_iterate_cur():
    with connect(creds) as conn:
        with conn.execute("select * from sample.employee") as cur:
            for _ in cur.fetchmany(5)["data"]:
                pass
            assert True
            return
    assert False


def test_pep249_nextset():
    conn = connect(creds)
    cur = conn.execute("select * from sample.employee")
    res = cur.nextset()
    assert res == None


def test_pep249_nextset_true():
    conn = connect(creds)
    cur = conn.cursor()
    cur.execute("select * from sample.employee")
    cur.execute("select * from sample.department")
    res = cur.nextset()
    assert res == True

    res = cur.nextset()
    assert res == None
    rows = cur.fetchmany(5)
    assert len(rows["data"]) == 5

    res = cur.nextset()
    assert res == None
    rows = cur.fetchmany(5)
    assert len(rows["data"]) == 5


def test_pep249_execute_many():
    conn = connect(creds)
    cur = conn.cursor()
    parameters = [
        ["SANJULA", "416 345 0879"],
        ["TONGKUN", "647 345 0879"],
        ["KATHERINE", "905 345 1879"],
        ["IRFAN", "647 345 0879"],
        ["SANJULA", "416 234 0879"],
        ["TONGKUN", "333 345 0879"],
        ["KATHERINE", "416 345 0000"],
        ["IRFAN", "416 345 3333"],
        ["SANJULA", "416 545 0879"],
        ["TONGKUN", "456 345 0879"],
        ["KATHERINE", "416 065 1879"],
        ["IRFAN", "416 345 1111"],
    ]
    cur.execute("drop table sample.deletemepy if exists")
    cur.execute("CREATE or replace TABLE SAMPLE.DELETEMEPY (name varchar(10), phone varchar(12))")
    assert len(cur.query_q) == 0
    res = cur.fetchall()
    assert res == None

    cur.executemany("INSERT INTO SAMPLE.DELETEMEPY values (?, ?)", parameters)

    assert cur.rowcount == 12

    cur.execute("select * from sample.deletemepy")

    res = cur.fetchall()

    assert len(res["data"]) == 12


def test_pep249_has_results():
    with connect(creds) as conn:
        cur = conn.cursor()
        cur.execute("select * from sample.department")
        assert cur.has_results == True

        cur.execute(
            "create or replace variable sample.coolval varchar(8) ccsid 1208 default 'abcd'"
        )

        assert cur.has_results == True

        assert cur.fetchall() is not None


def test_pep249_has_results_no_select():
    with connect(creds) as conn:
        cur = conn.cursor()
        cur.execute(
            "create or replace variable sample.coolval varchar(8) ccsid 1208 default 'abcd'"
        )

        assert cur.has_results == False

        assert cur.fetchall() is None


def test_pep249_has_results_setter():
    with connect(creds) as conn:
        cur = conn.cursor()
        cur.execute(
            "create or replace variable sample.coolval varchar(8) ccsid 1208 default 'abcd'"
        )

        assert cur.has_results == False

        assert cur.fetchall() is None


def test_pep249_has_results_flow():
    with connect(creds) as conn:
        with conn.execute("select * from sample.employee") as cur:
            if cur.has_results:
                assert cur.fetchall() is not None


def test_pep249_has_next_set_None():
    with connect(creds) as conn:
        with conn.execute("select * from sample.employee") as cur:
            if cur.has_results:
                assert cur.fetchall() is not None
            if not cur.nextset():
                cur.execute("select * from sample.department")
                assert cur.fetchone() is not None
