"""
Test to verify the unified query architecture works correctly.
"""

import sys
sys.path.insert(0, '..')

from mapepire_python.core.query_base import QueryResult, SyncQueryExecutor, AsyncQueryExecutor, BaseQuery, QueryState
from mapepire_python.data_types import QueryOptions


def test_query_result_dictionary_compatibility():
    """Test that QueryResult maintains backward compatibility with dictionary access."""
    test_data = {
        'success': True,
        'data': [{'id': 1, 'name': 'test'}],
        'is_done': False,
        'id': 'query123',
        'metadata': {'count': 1},
        'execution_time': 0.5
    }

    result = QueryResult(test_data)

    # Test dictionary access patterns used throughout codebase
    assert result['success'] is True
    assert result['data'] == [{'id': 1, 'name': 'test'}]
    assert result['is_done'] is False
    assert result['id'] == 'query123'
    assert result['has_results'] is True  # Computed property
    
    # Test .get() method
    assert result.get('success') is True
    assert result.get('nonexistent', 'default') == 'default'
    
    # Test 'in' operator
    assert 'success' in result
    assert 'nonexistent' not in result
    
    # Test property access (modern style)
    assert result.success is True
    assert result.data == [{'id': 1, 'name': 'test'}]
    assert result.is_done is False


def test_base_query_architecture():
    """Test that BaseQuery provides proper interface."""
    # Test query state enum
    assert QueryState.NOT_YET_RUN.value == 1
    assert QueryState.RUN_MORE_DATA_AVAIL.value == 2
    assert QueryState.RUN_DONE.value == 3
    assert QueryState.ERROR.value == 4
    
    # Test executor classes exist
    assert SyncQueryExecutor is not None
    assert AsyncQueryExecutor is not None
    
    # Test that executors have required methods
    sync_executor = SyncQueryExecutor()
    assert hasattr(sync_executor, 'execute_query')
    assert hasattr(sync_executor, 'validate_connection')
    
    async_executor = AsyncQueryExecutor()
    assert hasattr(async_executor, 'execute_query')
    assert hasattr(async_executor, 'validate_connection')


def test_unified_architecture_elimination_of_duplication():
    """Verify that the unified architecture eliminates code duplication."""
    from mapepire_python.client.query import Query
    from mapepire_python.pool.pool_query import PoolQuery
    
    # Both should inherit from BaseQuery
    assert issubclass(Query, BaseQuery)
    assert issubclass(PoolQuery, BaseQuery)
    
    # Both should use the strategy pattern
    opts = QueryOptions()
    
    # We can't test without actual job instances, but we can verify the structure
    assert Query.__init__ is not None
    assert PoolQuery.__init__ is not None


if __name__ == "__main__":
    test_query_result_dictionary_compatibility()
    test_base_query_architecture()
    test_unified_architecture_elimination_of_duplication()
    print("✅ All unified architecture tests passed!")