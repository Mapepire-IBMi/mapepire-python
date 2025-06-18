"""
Tests for mapepire_python.core.connection module.
Simple tests using real IBM i server.
"""

import pytest

from mapepire_python import connect
from mapepire_python.core.connection import Connection


def test_connection_creation(ibmi_credentials):
    """Test basic connection creation with IBM i server."""
    with connect(ibmi_credentials) as conn:
        assert conn is not None
        assert isinstance(conn, Connection)
        assert not conn._closed


def test_connection_context_manager(ibmi_credentials):
    """Test Connection as context manager."""
    conn = None
    with connect(ibmi_credentials) as connection:
        conn = connection
        assert not conn._closed

    # Connection should be closed after context exit
    assert conn._closed


def test_connection_cursor_creation(ibmi_credentials):
    """Test cursor creation from connection."""
    with connect(ibmi_credentials) as conn:
        cursor = conn.cursor()
        assert cursor is not None
        assert cursor.connection == conn


def test_connection_execute_shortcut(ibmi_credentials, simple_count_sql):
    """Test execute method on connection (shortcut to cursor.execute)."""
    with connect(ibmi_credentials) as conn:
        with conn.execute(simple_count_sql) as cursor:
            result = cursor.fetchone()
            assert result is not None
            assert len(result) == 1  # COUNT(*) returns one column


def test_connection_multiple_cursors(ibmi_credentials, simple_count_sql):
    """Test creating multiple cursors from same connection."""
    with connect(ibmi_credentials) as conn:
        cursor1 = conn.cursor()
        cursor2 = conn.cursor()

        assert cursor1 != cursor2
        assert cursor1.connection == conn
        assert cursor2.connection == conn


def test_connection_commit_rollback(ibmi_credentials):
    """Test commit and rollback operations."""
    with connect(ibmi_credentials) as conn:
        # These should not raise exceptions
        conn.commit()
        conn.rollback()


def test_connection_close_explicit(ibmi_credentials):
    """Test explicit connection close."""
    conn = connect(ibmi_credentials)
    assert not conn._closed

    conn.close()
    assert conn._closed


def test_connection_with_dict_credentials():
    """Test connection creation with dictionary credentials."""
    creds = {"host": "test-server", "port": 8076, "user": "testuser", "password": "testpass"}

    # This should fail with connection error, but not crash
    with pytest.raises(Exception):  # Could be various connection errors
        with connect(creds) as conn:
            pass


def test_connection_with_invalid_credentials():
    """Test connection with invalid credentials."""
    invalid_creds = {
        "host": "nonexistent-server",
        "port": 9999,
        "user": "invalid",
        "password": "invalid",
    }

    with pytest.raises(Exception):  # Connection should fail
        with connect(invalid_creds) as conn:
            pass
