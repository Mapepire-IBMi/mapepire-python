#!/usr/bin/env python3
"""
IBM i Error Discovery Script

This script connects to an IBM i system and deliberately triggers various types of errors
to capture and analyze the JSON error responses from the Mapepire server.

The goal is to understand the actual error response format and classification patterns
to improve the PEP 249 exception mapping in Phase 3.
"""

import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

# Add the project root to the path so we can import mapepire_python
sys.path.insert(0, os.path.abspath("."))

from mapepire_python.client.sql_job import SQLJob


class ErrorDiscovery:
    """Systematically trigger and catalog IBM i errors"""

    def __init__(self, credentials: Dict[str, str]):
        self.credentials = credentials
        self.errors_discovered = []
        self.timestamp = datetime.now().isoformat()

    def log_error(self, category: str, description: str, sql: str, error_details: Dict[str, Any]):
        """Log an error response for analysis"""
        error_entry = {
            "timestamp": datetime.now().isoformat(),
            "category": category,
            "description": description,
            "sql": sql,
            "error_details": error_details,
            "full_response": error_details,  # In case there are additional fields
        }
        self.errors_discovered.append(error_entry)
        print(f"[{category}] {description}")
        print(f"  SQL: {sql}")
        print(f"  Error: {json.dumps(error_details, indent=2)}")
        print("-" * 80)

    def safe_execute(self, job, category: str, description: str, sql: str) -> Optional[Dict[str, Any]]:
        """Safely execute SQL and capture any errors"""
        try:
            result = job.query_and_run(sql)
            if not result.get("success", True):
                self.log_error(category, description, sql, result)
            return result
        except Exception as e:
            # Capture any Python exceptions that might occur
            error_details = {"python_exception": str(e), "exception_type": type(e).__name__}
            self.log_error(category, f"{description} (Python Exception)", sql, error_details)
            return None

    def test_programming_errors(self, job):
        """Test SQL syntax and programming errors"""
        print("\n=== PROGRAMMING ERRORS ===")

        # SQL syntax errors
        self.safe_execute(job, "PROGRAMMING", "Invalid SQL syntax", "SELEC * FROM NONEXISTENT")

        self.safe_execute(job, "PROGRAMMING", "Missing FROM clause", "SELECT column1")

        self.safe_execute(
            job, "PROGRAMMING", "Invalid column name", "SELECT invalid_column FROM SYSIBM.SYSDUMMY1"
        )

        # Object not found errors
        self.safe_execute(job, "PROGRAMMING", "Table not found", "SELECT * FROM NONEXISTENT_TABLE")

        self.safe_execute(
            job, "PROGRAMMING", "Schema not found", "SELECT * FROM NONEXISTENT_SCHEMA.SOME_TABLE"
        )

        self.safe_execute(
            job,
            "PROGRAMMING",
            "Function not found",
            "SELECT NONEXISTENT_FUNCTION() FROM SYSIBM.SYSDUMMY1",
        )

        # Access/permission errors
        self.safe_execute(
            job,
            "PROGRAMMING",
            "No authority to object",
            "SELECT * FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA = 'QSYS'",
        )

    def test_data_errors(self, job):
        """Test data validation and conversion errors"""
        print("\n=== DATA ERRORS ===")

        # Data type conversion errors
        self.safe_execute(
            job,
            "DATA",
            "Invalid numeric conversion",
            "SELECT CAST('not_a_number' AS INTEGER) FROM SYSIBM.SYSDUMMY1",
        )

        self.safe_execute(
            job,
            "DATA",
            "Invalid date format",
            "SELECT CAST('invalid_date' AS DATE) FROM SYSIBM.SYSDUMMY1",
        )

        self.safe_execute(
            job,
            "DATA",
            "Numeric overflow",
            "SELECT CAST(99999999999999999999 AS SMALLINT) FROM SYSIBM.SYSDUMMY1",
        )

        self.safe_execute(job, "DATA", "Division by zero", "SELECT 1/0 FROM SYSIBM.SYSDUMMY1")

        # String/character errors
        self.safe_execute(
            job,
            "DATA",
            "String too long for field",
            "SELECT CAST(REPEAT('A', 1000) AS CHAR(10)) FROM SYSIBM.SYSDUMMY1",
        )

    def test_operational_errors(self, job):
        """Test operational and resource errors"""
        print("\n=== OPERATIONAL ERRORS ===")

        # Try to access system objects that might cause operational errors
        self.safe_execute(
            job,
            "OPERATIONAL",
            "System resource access",
            "SELECT * FROM QSYS2.ACTIVE_JOB_INFO WHERE JOB_NAME = 'NONEXISTENT'",
        )

        # Try operations that might timeout or cause locks
        self.safe_execute(
            job,
            "OPERATIONAL",
            "Potential locking issue",
            "LOCK TABLE QSYS2.SYSTABLES IN EXCLUSIVE MODE",
        )

    def test_integrity_errors(self, job):
        """Test integrity constraint violations"""
        print("\n=== INTEGRITY ERRORS ===")

        # We'll create a temporary table to test constraints
        # First, try to create a test table
        table_name = f"MAPEPIRE_TEST_{datetime.now().strftime('%H%M%S')}"

        # Create table with constraints
        create_sql = f"""
        CREATE TABLE QTEMP.{table_name} (
            ID INTEGER PRIMARY KEY,
            NAME VARCHAR(50) NOT NULL,
            EMAIL VARCHAR(100) UNIQUE
        )
        """

        result = self.safe_execute(job, "SETUP", "Create test table", create_sql)

        if result and result.get("success", False):
            # Test NOT NULL constraint
            self.safe_execute(
                job,
                "INTEGRITY",
                "NOT NULL constraint violation",
                f"INSERT INTO QTEMP.{table_name} (ID, NAME) VALUES (1, NULL)",
            )

            # Test PRIMARY KEY constraint
            self.safe_execute(
                job,
                "INTEGRITY",
                "Primary key constraint",
                f"INSERT INTO QTEMP.{table_name} (ID, NAME) VALUES (1, 'Test1')",
            )
            self.safe_execute(
                job,
                "INTEGRITY",
                "Primary key violation",
                f"INSERT INTO QTEMP.{table_name} (ID, NAME) VALUES (1, 'Test2')",
            )

            # Test UNIQUE constraint
            self.safe_execute(
                job,
                "INTEGRITY",
                "Unique constraint setup",
                f"INSERT INTO QTEMP.{table_name} (ID, NAME, EMAIL) VALUES (2, 'Test2', 'test@example.com')",
            )
            self.safe_execute(
                job,
                "INTEGRITY",
                "Unique constraint violation",
                f"INSERT INTO QTEMP.{table_name} (ID, NAME, EMAIL) VALUES (3, 'Test3', 'test@example.com')",
            )

            # Cleanup
            self.safe_execute(job, "CLEANUP", "Drop test table", f"DROP TABLE QTEMP.{table_name}")

    def test_connection_errors(self):
        """Test connection-related errors"""
        print("\n=== CONNECTION ERRORS ===")

        # Test invalid credentials
        invalid_creds = self.credentials.copy()
        invalid_creds["password"] = "invalid_password"

        try:
            job = SQLJob()
            job.connect(invalid_creds)
            print("ERROR: Should have failed with invalid credentials")
        except Exception as e:
            error_details = {"python_exception": str(e), "exception_type": type(e).__name__}
            self.log_error("CONNECTION", "Invalid credentials", "N/A (connection)", error_details)

        # Test invalid server
        invalid_server_creds = self.credentials.copy()
        invalid_server_creds["host"] = "nonexistent.server.com"

        try:
            job = SQLJob()
            job.connect(invalid_server_creds)
            print("ERROR: Should have failed with invalid server")
        except Exception as e:
            error_details = {"python_exception": str(e), "exception_type": type(e).__name__}
            self.log_error("CONNECTION", "Invalid server", "N/A (connection)", error_details)

    def discover_errors(self) -> List[Dict[str, Any]]:
        """Main method to discover all error types"""
        print(f"Starting IBM i Error Discovery at {self.timestamp}")
        print("=" * 80)

        # Test connection errors first
        self.test_connection_errors()

        # Test with valid connection
        try:
            job = SQLJob()
            job.connect(self.credentials)
            print(f"Successfully connected to {self.credentials['host']}")

            # Run all error discovery tests
            self.test_programming_errors(job)
            self.test_data_errors(job)
            self.test_operational_errors(job)
            self.test_integrity_errors(job)

            job.close()

        except Exception as e:
            print(f"Failed to establish connection: {e}")
            return self.errors_discovered

        print(f"\nDiscovery completed. Found {len(self.errors_discovered)} error responses.")
        return self.errors_discovered

    def save_results(self, filename: str = None):
        """Save discovered errors to JSON file"""
        if filename is None:
            filename = f"ibm_i_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        results = {
            "discovery_timestamp": self.timestamp,
            "total_errors": len(self.errors_discovered),
            "errors": self.errors_discovered,
        }

        with open(filename, "w") as f:
            json.dump(results, f, indent=2)

        print(f"Results saved to {filename}")
        return filename


def main():
    """Main execution function"""
    from dotenv import load_dotenv

    load_dotenv()
    # Get IBM i credentials from environment variables
    credentials = {
        "host": os.getenv("VITE_SERVER"),
        "user": os.getenv("VITE_DB_USER"),
        "password": os.getenv("VITE_DB_PASS"),
        "port": int(os.getenv("VITE_DB_PORT", "8076")),
    }

    # Validate credentials
    missing_creds = [k for k, v in credentials.items() if v is None]
    if missing_creds:
        print(f"Missing required environment variables: {missing_creds}")
        print("Please set: VITE_SERVER, VITE_DB_USER, VITE_DB_PASS")
        print("Optional: VITE_DB_PORT (defaults to 8076)")
        sys.exit(1)

    print(f"Connecting to IBM i at {credentials['host']}:{credentials['port']}")
    print(f"Username: {credentials['user']}")

    # Run error discovery
    discovery = ErrorDiscovery(credentials)
    errors = discovery.discover_errors()

    # Save results
    filename = discovery.save_results()

    # Print summary
    print("\n" + "=" * 80)
    print("ERROR DISCOVERY SUMMARY")
    print("=" * 80)

    categories = {}
    for error in errors:
        cat = error["category"]
        if cat not in categories:
            categories[cat] = 0
        categories[cat] += 1

    for category, count in categories.items():
        print(f"{category}: {count} errors")

    print(f"\nDetailed results saved to: {filename}")
    print("\nNext steps:")
    print("1. Review the error responses in the JSON file")
    print("2. Update the IBM i error classification mappings")
    print("3. Implement the enhanced exception hierarchy")


if __name__ == "__main__":
    main()
