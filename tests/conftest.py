"""
Shared pytest fixtures for mapepire-python tests.
Following Occam's Razor: Simple fixtures for IBM i server testing.
"""

import os
import pytest
from typing import Dict, Any


@pytest.fixture(scope="session")
def ibmi_credentials() -> Dict[str, Any]:
    """IBM i server credentials from environment variables."""
    return {
        "host": os.getenv("VITE_SERVER"),
        "port": int(os.getenv("VITE_DB_PORT", "8076")),
        "user": os.getenv("VITE_DB_USER"),
        "password": os.getenv("VITE_DB_PASS")
    }


@pytest.fixture
def sample_employee_sql() -> str:
    """Standard employee table query for testing."""
    return "SELECT * FROM sample.employee LIMIT 5"


@pytest.fixture  
def sample_department_sql() -> str:
    """Standard department table query for testing."""
    return "SELECT * FROM sample.department"


@pytest.fixture
def simple_count_sql() -> str:
    """Simple count query for testing."""
    return "SELECT COUNT(*) as total_count FROM sample.employee"