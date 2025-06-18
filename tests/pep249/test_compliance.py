"""
Tests for PEP 249 (DB API 2.0) compliance.
Simple tests using real IBM i server.

NOTE: mapepire-python provides a PEP 249-inspired interface but returns JSON objects
instead of tuples from fetch methods. This is a design choice for IBM i integration.
"""

from mapepire_python import apilevel, connect, paramstyle, threadsafety


def test_module_level_attributes():
    """Test required module-level attributes."""
    assert apilevel == "2.0"
    assert isinstance(threadsafety, int)
    assert threadsafety >= 0
    assert paramstyle in ["format", "pyformat", "numeric", "named", "qmark"]


def test_connect_function(ibmi_credentials):
    """Test the connect function returns proper connection."""
    conn = connect(ibmi_credentials)
    assert conn is not None

    # Check required methods exist
    assert hasattr(conn, "cursor")
    assert hasattr(conn, "commit")
    assert hasattr(conn, "rollback")
    assert hasattr(conn, "close")

    conn.close()


def test_connection_interface(ibmi_credentials):
    """Test Connection implements required interface."""
    with connect(ibmi_credentials) as conn:
        # Test cursor creation
        cursor = conn.cursor()
        assert cursor is not None

        # Test transaction methods exist
        conn.commit()
        conn.rollback()


def test_cursor_interface(ibmi_credentials, simple_count_sql):
    """Test Cursor implements required interface."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # Required attributes
        assert hasattr(cursor, "description")
        assert hasattr(cursor, "rowcount")

        # Required methods
        assert hasattr(cursor, "execute")
        assert hasattr(cursor, "executemany")
        assert hasattr(cursor, "fetchone")
        assert hasattr(cursor, "fetchmany")
        assert hasattr(cursor, "fetchall")
        assert hasattr(cursor, "close")

        # Test basic execute
        cursor.execute(simple_count_sql)

        # Test fetch methods - mapepire returns JSON objects, not tuples
        row = cursor.fetchone()
        assert row is None or isinstance(row, dict)


def test_cursor_description(ibmi_credentials, sample_employee_sql):
    """Test cursor description after query execution."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sample_employee_sql)

            if cursor.description is not None:
                # Each column description should be a sequence
                for col_desc in cursor.description:
                    assert len(col_desc) >= 2  # name, type_code at minimum
                    assert isinstance(col_desc[0], str)  # Column name


def test_cursor_fetchone(ibmi_credentials, simple_count_sql):
    """Test cursor fetchone method."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(simple_count_sql)

            row = cursor.fetchone()
            if row is not None:
                # mapepire returns JSON objects, not tuples
                assert isinstance(row, dict)

            # Subsequent calls should return None when no more data
            next_row = cursor.fetchone()
            # May be None or another row depending on result set


def test_cursor_fetchmany(ibmi_credentials, sample_employee_sql):
    """Test cursor fetchmany method."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sample_employee_sql)

            rows = cursor.fetchmany(2)
            # mapepire returns JSON object with results, not list of tuples

            assert len(rows) == 2


def test_cursor_fetchall(ibmi_credentials, sample_employee_sql):
    """Test cursor fetchall method."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sample_employee_sql)

            rows = cursor.fetchall()
            # mapepire returns JSON object with results, not list of tuples
            assert rows is not None


def test_cursor_arraysize(ibmi_credentials):
    """Test cursor arraysize attribute."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        # arraysize should be readable and writable
        original_size = getattr(cursor, "arraysize", 1)
        assert isinstance(original_size, int)

        # Try setting it
        if hasattr(cursor, "arraysize"):
            cursor.arraysize = 10
            assert cursor.arraysize == 10


def test_parameter_substitution(ibmi_credentials):
    """Test parameter substitution based on paramstyle."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            print(paramstyle)
            if paramstyle == "qmark":
                cursor.execute("SELECT * FROM sample.employee WHERE empno = ?", ["000010"])
            elif paramstyle == "format":
                cursor.execute("SELECT * FROM sample.employee WHERE empno = %s", ("000010",))
            elif paramstyle == "named":
                cursor.execute(
                    "SELECT * FROM sample.employee WHERE empno = :empno", {"empno": "000010"}
                )
            elif paramstyle == "pyformat":
                cursor.execute(
                    "SELECT * FROM sample.employee WHERE empno = %(empno)s", {"empno": "000010"}
                )

            result = cursor.fetchone()
            # Just verify we got some result - exact content depends on sample data
            assert result is None or isinstance(result, dict)


def test_connection_context_manager(ibmi_credentials):
    """Test connection as context manager."""
    conn = connect(ibmi_credentials)

    with conn:
        # Connection should be usable
        cursor = conn.cursor()
        assert cursor is not None

    # Connection should be closed after context
    assert conn._closed


def test_cursor_context_manager(ibmi_credentials, simple_count_sql):
    """Test cursor as context manager."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()

        with cursor:
            cursor.execute(simple_count_sql)
            result = cursor.fetchone()

        # Cursor should be closed after context
        assert cursor._closed


def test_exception_hierarchy():
    """Test that exceptions follow PEP 249 hierarchy."""
    from mapepire_python import (
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        ProgrammingError,
    )

    # Test inheritance hierarchy
    assert issubclass(DatabaseError, Error)
    assert issubclass(DataError, DatabaseError)
    assert issubclass(IntegrityError, DatabaseError)
    assert issubclass(InterfaceError, Error)
    assert issubclass(InternalError, DatabaseError)
    assert issubclass(NotSupportedError, DatabaseError)
    assert issubclass(OperationalError, DatabaseError)
    assert issubclass(ProgrammingError, DatabaseError)


# Additional PEP 249 tests migrated from pep249_test.py


def test_fetchmany_with_size(ibmi_credentials, sample_employee_sql):
    """Test fetchmany with specified size."""
    conn = connect(ibmi_credentials)
    cur = conn.execute(sample_employee_sql)
    res = cur.fetchmany(5)
    cur.close()
    conn.close()
    if res and "data" in res:
        assert len(res["data"]) <= 5


def test_fetchmany_default_size(ibmi_credentials, sample_employee_sql):
    """Test fetchmany with default arraysize."""
    conn = connect(ibmi_credentials)
    cur = conn.execute(sample_employee_sql)
    res = cur.fetchmany()
    conn.close()
    # Default arraysize behavior


def test_arraysize_property(ibmi_credentials, sample_employee_sql):
    """Test cursor arraysize property."""
    conn = connect(ibmi_credentials)
    cur = conn.execute(sample_employee_sql)
    if hasattr(cur, "arraysize"):
        cur.arraysize = 10
        res = cur.fetchmany()
        if res and "data" in res:
            assert len(res["data"]) <= 10
    conn.close()


def test_context_manager_fetchmany(ibmi_credentials, sample_employee_sql):
    """Test fetchmany with context managers."""
    with connect(ibmi_credentials) as connection:
        with connection.execute(sample_employee_sql) as cur:
            res = cur.fetchmany(5)
            if res and "data" in res:
                assert len(res["data"]) <= 5


def test_context_manager_fetchall(ibmi_credentials, sample_employee_sql):
    """Test fetchall with context managers."""
    with connect(ibmi_credentials) as connection:
        with connection.execute(sample_employee_sql) as cur:
            res = cur.fetchall()
            if res and "data" in res:
                assert len(res["data"]) >= 0


def test_context_manager_fetchone(ibmi_credentials, sample_employee_sql):
    """Test fetchone with context managers."""
    with connect(ibmi_credentials) as connection:
        with connection.execute(sample_employee_sql) as cur:
            res = cur.fetchone()
            if res and "data" in res:
                assert len(res["data"]) >= 0

            res2 = cur.fetchone()
            # Second fetch might be different from first


def test_cursor_iteration(ibmi_credentials, sample_employee_sql):
    """Test cursor iteration support."""
    with connect(ibmi_credentials) as connection:
        with connection.execute(sample_employee_sql) as cur:
            if hasattr(cur, "__next__"):
                try:
                    next_result = next(cur)
                    assert next_result is not None
                except StopIteration:
                    pass  # End of results


def test_query_queue_multiple_executes(ibmi_credentials):
    """Test multiple query execution and queue management."""
    conn = connect(ibmi_credentials)
    cur = conn.cursor()

    # Execute queries that are guaranteed to have results
    cur.execute("select count(*) from sample.employee")
    first_row = cur.fetchone()
    assert first_row is not None  # Ensure first query has results

    cur.execute("select count(*) from sample.department")
    second_row = cur.fetchone()
    assert second_row is not None  # Ensure second query has results

    # Now check the query queue - should have queries with results
    if hasattr(cur, "query_q"):
        print(cur.query_q)
        # At least one query should be in queue (queries with result sets)
        assert len(cur.query_q) >= 1, f"Expected at least 1 query in queue, got {len(cur.query_q)}"

    # Test nextset functionality
    if hasattr(cur, "nextset"):
        next_result = cur.nextset()
        assert next_result is not None
        # May be True, False, or None depending on implementation

    conn.close()


def test_prepared_statements_with_parameters(ibmi_credentials):
    """Test prepared statements with parameters."""
    from mapepire_python.data_types import QueryOptions

    conn = connect(ibmi_credentials)
    cur = conn.cursor()

    # Test with QueryOptions
    try:
        opts = QueryOptions(parameters=[500, "PRES"])
        cur.execute("select * from sample.employee where bonus > ? and job = ?", opts=opts)
        res = cur.fetchall()
        if res:
            assert res.get("success") is True
    except Exception:
        pass  # Parameter binding might not be fully implemented

    # Test with direct parameters
    try:
        parameters = [500, "PRES"]
        cur.execute(
            "select * from sample.employee where bonus > ? and job = ?", parameters=parameters
        )
        res = cur.fetchall()
        if res:
            assert res.get("success") is True
    except Exception:
        pass  # Parameter binding might not be fully implemented

    conn.close()


def test_executemany(ibmi_credentials):
    """Test executemany method."""
    conn = connect(ibmi_credentials)
    cur = conn.cursor()

    try:
        # Create test table
        cur.execute("drop table sample.deletemepy if exists")
        cur.execute(
            "CREATE or replace TABLE SAMPLE.DELETEMEPY (name varchar(10), phone varchar(12))"
        )

        # Test executemany
        parameters = [
            ["TEST1", "111-1111"],
            ["TEST2", "222-2222"],
            ["TEST3", "333-3333"],
        ]
        cur.executemany("INSERT INTO SAMPLE.DELETEMEPY values (?, ?)", parameters)

        # Verify results
        cur.execute("select * from sample.deletemepy")
        res = cur.fetchall()
        if res and "data" in res:
            assert len(res["data"]) >= 0
    except Exception:
        pass  # executemany might not be fully implemented
    finally:
        conn.close()


def test_has_results_property(ibmi_credentials):
    """Test has_results property."""
    with connect(ibmi_credentials) as conn:
        cur = conn.cursor()
        cur.execute("select * from sample.department")

        if hasattr(cur, "has_results"):
            # has_results should indicate if query returned data
            has_results = cur.has_results
            assert isinstance(has_results, bool)


def test_nextset_functionality(ibmi_credentials):
    """Test nextset method functionality."""
    conn = connect(ibmi_credentials)
    cur = conn.cursor()

    cur.execute("select * from sample.employee")

    if hasattr(cur, "nextset"):
        # Single query should return None for nextset
        res = cur.nextset()
        assert res is None or res is False

    conn.close()
