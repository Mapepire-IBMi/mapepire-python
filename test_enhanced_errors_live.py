#!/usr/bin/env python3
"""
Test Enhanced Error Handling with Live IBM i Connection

This script tests the enhanced error handling with real IBM i errors
to ensure the @convert_runtime_errors decorator works end-to-end.
"""

import os
import sys
from dotenv import load_dotenv

# Add the project root to the path so we can import mapepire_python
sys.path.insert(0, os.path.abspath('.'))

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.core.exceptions import DatabaseError


def test_live_enhanced_errors():
    """Test enhanced error handling with real IBM i connection"""
    
    load_dotenv()
    
    # Get IBM i credentials
    credentials = {
        "host": os.getenv("VITE_SERVER"),
        "user": os.getenv("VITE_DB_USER"),
        "password": os.getenv("VITE_DB_PASS"),
        "port": int(os.getenv("VITE_DB_PORT", "8076")),
    }
    
    # Validate credentials
    missing_creds = [k for k, v in credentials.items() if v is None]
    if missing_creds:
        print(f"Missing credentials: {missing_creds}")
        print("Please set VITE_SERVER, VITE_DB_USER, VITE_DB_PASS environment variables")
        return
    
    print("Testing Enhanced Error Handling with Live IBM i")
    print("=" * 60)
    print(f"Connected to: {credentials['host']}")
    print()
    
    # Test cases that will trigger different error types
    test_queries = [
        {
            'name': 'Table Not Found Error',
            'sql': 'SELECT * FROM NONEXISTENT_TABLE',
            'expected_pattern': 'table not found'
        },
        {
            'name': 'Column Not Found Error', 
            'sql': 'SELECT invalid_column FROM SYSIBM.SYSDUMMY1',
            'expected_pattern': 'Column'
        },
        {
            'name': 'Syntax Error',
            'sql': 'SELEC * FROM SYSIBM.SYSDUMMY1',
            'expected_pattern': 'Syntax error'
        },
        {
            'name': 'Function Not Found',
            'sql': 'SELECT NONEXISTENT_FUNC() FROM SYSIBM.SYSDUMMY1',
            'expected_pattern': 'function not found'
        }
    ]
    
    try:
        with SQLJob(credentials) as job:
            print("✓ Successfully connected to IBM i")
            print()
            
            for i, test in enumerate(test_queries, 1):
                print(f"Test {i}: {test['name']}")
                print("-" * 40)
                print(f"SQL: {test['sql']}")
                
                try:
                    # This should trigger our enhanced error handling
                    result = job.query_and_run(test['sql'])
                    print(f"❌ Expected error but got success: {result}")
                    
                except DatabaseError as e:
                    print(f"✓ Enhanced Error Message:")
                    print(f"  {e}")
                    print(f"✓ Exception Type: {type(e).__name__}")
                    
                    # Check if our enhancement worked
                    error_str = str(e)
                    if test['expected_pattern'].lower() in error_str.lower():
                        print(f"✓ Contains expected pattern: '{test['expected_pattern']}'")
                    else:
                        print(f"⚠️  Expected pattern '{test['expected_pattern']}' not found")
                    
                    # Show debugging attributes
                    if hasattr(e, 'sql_state') and e.sql_state:
                        print(f"  SQL State: {e.sql_state}")
                    if hasattr(e, 'sql_code') and e.sql_code:
                        print(f"  SQL Code: {e.sql_code}")
                    
                except Exception as e:
                    print(f"❌ Unexpected exception type: {type(e).__name__}: {e}")
                
                print()
    
    except Exception as e:
        print(f"❌ Connection failed: {e}")
    
    print("=" * 60)
    print("Live error handling test completed!")


def test_successful_query():
    """Test that successful queries still work normally"""
    
    load_dotenv()
    
    credentials = {
        "host": os.getenv("VITE_SERVER"),
        "user": os.getenv("VITE_DB_USER"),
        "password": os.getenv("VITE_DB_PASS"),
        "port": int(os.getenv("VITE_DB_PORT", "8076")),
    }
    
    missing_creds = [k for k, v in credentials.items() if v is None]
    if missing_creds:
        return
    
    print("\nTesting Successful Query (No Error Enhancement)")
    print("=" * 50)
    
    try:
        with SQLJob(credentials) as job:
            # Test a query that should succeed
            result = job.query_and_run("SELECT 1 as test_column FROM SYSIBM.SYSDUMMY1")
            
            if result.get('success', False):
                print("✓ Successful query completed normally")
                print(f"  Result: {result.get('data', [])}")
            else:
                print(f"❌ Query failed unexpectedly: {result}")
                
    except Exception as e:
        print(f"❌ Unexpected error in successful query: {e}")


if __name__ == "__main__":
    test_live_enhanced_errors()
    test_successful_query()