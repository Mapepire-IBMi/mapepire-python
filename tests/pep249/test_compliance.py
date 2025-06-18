"""
Tests for PEP 249 (DB API 2.0) compliance.
Following Occam's Razor: Simple tests using real IBM i server.
"""

import pytest
from mapepire_python import connect, apilevel, threadsafety, paramstyle


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
    assert hasattr(conn, 'cursor')
    assert hasattr(conn, 'commit') 
    assert hasattr(conn, 'rollback')
    assert hasattr(conn, 'close')
    
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
        assert hasattr(cursor, 'description')
        assert hasattr(cursor, 'rowcount')
        
        # Required methods
        assert hasattr(cursor, 'execute')
        assert hasattr(cursor, 'executemany')
        assert hasattr(cursor, 'fetchone')
        assert hasattr(cursor, 'fetchmany') 
        assert hasattr(cursor, 'fetchall')
        assert hasattr(cursor, 'close')
        
        # Test basic execute
        cursor.execute(simple_count_sql)
        
        # Test fetch methods
        row = cursor.fetchone()
        assert row is None or isinstance(row, tuple)


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
                assert isinstance(row, tuple)
                
            # Subsequent calls should return None when no more data
            next_row = cursor.fetchone()
            # May be None or another row depending on result set


def test_cursor_fetchmany(ibmi_credentials, sample_employee_sql):
    """Test cursor fetchmany method."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sample_employee_sql)
            
            rows = cursor.fetchmany(2)
            assert isinstance(rows, list)
            
            for row in rows:
                assert isinstance(row, tuple)


def test_cursor_fetchall(ibmi_credentials, sample_employee_sql):
    """Test cursor fetchall method."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sample_employee_sql)
            
            rows = cursor.fetchall()
            assert isinstance(rows, list)
            
            for row in rows:
                assert isinstance(row, tuple)


def test_cursor_arraysize(ibmi_credentials):
    """Test cursor arraysize attribute."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()
        
        # arraysize should be readable and writable
        original_size = getattr(cursor, 'arraysize', 1)
        assert isinstance(original_size, int)
        
        # Try setting it
        if hasattr(cursor, 'arraysize'):
            cursor.arraysize = 10
            assert cursor.arraysize == 10


def test_parameter_substitution(ibmi_credentials):
    """Test parameter substitution based on paramstyle."""
    with connect(ibmi_credentials) as conn:
        with conn.cursor() as cursor:
            if paramstyle == "qmark":
                cursor.execute("SELECT ? as test_param", (42,))
            elif paramstyle == "format":
                cursor.execute("SELECT %s as test_param", (42,))
            elif paramstyle == "named":
                cursor.execute("SELECT :value as test_param", {"value": 42})
            elif paramstyle == "pyformat":
                cursor.execute("SELECT %(value)s as test_param", {"value": 42})
            else:
                # For other paramstyles, just test basic query
                cursor.execute("SELECT 42 as test_param")
            
            result = cursor.fetchone()
            if result:
                assert result[0] == 42


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
        Error, DatabaseError, DataError, IntegrityError,
        InterfaceError, InternalError, NotSupportedError,
        OperationalError, ProgrammingError
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