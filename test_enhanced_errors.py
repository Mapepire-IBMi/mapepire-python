#!/usr/bin/env python3
"""
Test Enhanced Error Handling

This script tests the enhanced error handling implementation to ensure
user-friendly error messages are generated from IBM i error responses.
"""

import sys
import os

# Add the project root to the path so we can import mapepire_python
sys.path.insert(0, os.path.abspath('.'))

from mapepire_python.core.exceptions import _parse_runtime_error


def test_error_patterns():
    """Test various error patterns from our IBM i error discovery"""
    
    print("Testing Enhanced Error Handling")
    print("=" * 50)
    
    # Test cases based on actual error discovery results
    test_cases = [
        {
            'name': 'Table Not Found (Pattern 1 - Direct Dict)',
            'error': "{'error': '[SQL0204] NONEXISTENT_TABLE in MAPEPIRE type *FILE not found.', 'sql_state': '42704', 'sql_rc': -204}"
        },
        {
            'name': 'Syntax Error (Pattern 2 - SQLJob format)',
            'error': "Failed to run query: SELEC * FROM NONEXISTENT - {'error': '[SQL0104] Token SELEC was not valid. Valid tokens: ( CL END GET SET TAG CALL DROP FREE HOLD LOCK OPEN WITH ALTER.', 'sql_state': '42601', 'sql_rc': -104}"
        },
        {
            'name': 'Column Not Found',
            'error': "Failed to run query: SELECT invalid_column FROM SYSIBM.SYSDUMMY1 - {'error': '[SQL0206] Column or global variable INVALID_COLUMN not found.', 'sql_state': '42703', 'sql_rc': -206}"
        },
        {
            'name': 'Function Not Found',
            'error': "Failed to run query: SELECT NONEXISTENT_FUNCTION() FROM SYSIBM.SYSDUMMY1 - {'error': '[SQL0204] NONEXISTENT_FUNCTION in *LIBL type *N not found.', 'sql_state': '42704', 'sql_rc': -204}"
        },
        {
            'name': 'Operation Not Valid',
            'error': "Failed to run query: CREATE TABLE QTEMP.TEST_TABLE (ID INTEGER) - {'error': '[SQL7008] TEST_TABLE in QTEMP not valid for operation.', 'sql_state': '55019', 'sql_rc': -7008}"
        },
        {
            'name': 'Object Type Error',
            'error': "Failed to run query: LOCK TABLE QSYS2.SYSTABLES IN EXCLUSIVE MODE - {'error': '[SQL0156] SYSTABLES in QSYS2 not correct type.', 'sql_state': '42809', 'sql_rc': -156}"
        },
        {
            'name': 'Generic RuntimeError (Fallback)',
            'error': "Some unexpected error without structured data"
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test_case['name']}")
        print("-" * 40)
        
        try:
            # Create RuntimeError and test conversion
            runtime_error = RuntimeError(test_case['error'])
            enhanced_error = _parse_runtime_error(runtime_error)
            
            print(f"Original: {test_case['error'][:100]}{'...' if len(test_case['error']) > 100 else ''}")
            print(f"Enhanced: {enhanced_error}")
            print(f"Type: {type(enhanced_error).__name__}")
            
            # Show additional attributes if available
            if hasattr(enhanced_error, 'sql_state') and enhanced_error.sql_state:
                print(f"SQL State: {enhanced_error.sql_state}")
            if hasattr(enhanced_error, 'sql_code') and enhanced_error.sql_code:
                print(f"SQL Code: {enhanced_error.sql_code}")
            if hasattr(enhanced_error, 'context_sql') and enhanced_error.context_sql:
                print(f"Context SQL: {enhanced_error.context_sql}")
                
        except Exception as e:
            print(f"ERROR in test: {e}")
    
    print(f"\n{'=' * 50}")
    print("Enhanced error handling test completed!")


def test_real_usage_example():
    """Show how the enhanced errors work in real usage scenarios"""
    
    print(f"\n{'=' * 50}")
    print("REAL USAGE EXAMPLE")
    print("=" * 50)
    
    # Simulate how this would work with @convert_runtime_errors decorator
    from mapepire_python.core.exceptions import convert_runtime_errors
    
    @convert_runtime_errors
    def simulate_query_execution(sql: str, should_fail: bool = True):
        """Simulate a query execution that raises RuntimeError"""
        if should_fail:
            # This simulates the current RuntimeError pattern from SQLJob.query_and_run
            error_dict = {
                'error': '[SQL0204] EMPLOYEES in SAMPLE type *FILE not found.',
                'sql_state': '42704',
                'sql_rc': -204
            }
            raise RuntimeError(f"Failed to run query: {sql} - {error_dict}")
        return {"success": True, "data": []}
    
    # Test the decorator integration
    try:
        result = simulate_query_execution("SELECT * FROM SAMPLE.EMPLOYEES")
        print(f"Success: {result}")
    except Exception as e:
        print(f"Exception Type: {type(e).__name__}")
        print(f"User-Friendly Message: {e}")
        if hasattr(e, 'sql_state'):
            print(f"For Debugging - SQL State: {e.sql_state}")
        if hasattr(e, 'sql_code'):
            print(f"For Debugging - SQL Code: {e.sql_code}")


if __name__ == "__main__":
    test_error_patterns()
    test_real_usage_example()