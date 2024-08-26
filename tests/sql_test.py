import os
import re

import pytest

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.types import DaemonServer, QueryOptions

# Fetch environment variables
server = os.getenv('VITE_SERVER')
user = os.getenv('VITE_DB_USER')
password = os.getenv('VITE_DB_PASS')
port = os.getenv('VITE_DB_PORT')

# Check if environment variables are set
if not server or not user or not password:
    raise ValueError('One or more environment variables are missing.')


creds = DaemonServer(
    host=server,
    port=port,
    user=user,
    password=password,
    ignoreUnauthorized=True,
)

def test_simple():
    job = SQLJob()
    _ = job.connect(creds)
    query = job.query('select * from sample.employee')
    result = query.run(rows_to_fetch=5)
    job.close()
    assert result['success'] == True
    assert result['is_done'] == False
    assert result['has_results'] == True
    
def test_query_large_dataset():
    job = SQLJob()
    _ = job.connect(creds)
    query = job.query('select * from sample.employee')
    result = query.run(rows_to_fetch=30)
    job.close()
    
    assert result['success'] == True
    assert result['is_done'] == False
    assert result['has_results'] == True
    assert len(result['data']) == 30
    
def test_run_query_terse_format():
    job = SQLJob()
    _ = job.connect(creds)
    opts = QueryOptions(
        isTerseResults=True
    )
    query = job.query('select * from sample.employee', opts=opts)
    result = query.run(rows_to_fetch=5)
    job.close()
    
    assert result['success'] == True
    assert result['is_done'] == False
    assert result['has_results'] == True
    assert 'metadata' in result and result['metadata'], "The 'metadata' key is missing or has no data"
    
    
def test_invalid_query():
    job = SQLJob()
    _ = job.connect(creds)
    query = job.query('select * from sample.notreal')
    try:
        result = query.run(rows_to_fetch=5)
        raise Exception("error not raised")
    except Exception as e:
        assert e.args[0]
        assert 'error' in e.args[0]
        assert '*FILE not found.'  in e.args[0]['error']
    job.close()
    
def test_query_edge_cases():
    job = SQLJob()
    _ = job.connect(creds)
    query = job.query('')  # empty string
    try:
        _ = query.run(rows_to_fetch=1)
        raise Exception('no error raised')
    except Exception as e:
        assert e.args[0]
        assert 'error' in e.args[0]
        assert 'A string parameter value with zero length was detected.' in e.args[0]['error']
    
    
    
def test_run_sql_query_with_edge_case_inputs():
    job = SQLJob()
    job.connect(creds)
    
    # Test empty string query
    query = job.query('')
    with pytest.raises(Exception) as excinfo:
        query.run(rows_to_fetch=1)
    assert 'A string parameter value with zero length was detected.' in str(excinfo.value)
    
    # Test non-string query
    with pytest.raises(Exception) as excinfo:
        query = job.query(666)
        query.run(rows_to_fetch=1)
    assert 'Token 666 was not valid' in str(excinfo.value)
    
    # Test invalid token query
    query = job.query('a')
    with pytest.raises(Exception) as excinfo:
        query.run(rows_to_fetch=1)
    assert 'Token A was not valid.' in str(excinfo.value)
    
    # Test long invalid token query
    long_invalid_query = "aeriogfj304tq34projqwe'fa;sdfaSER90Q243RSDASDAFQ#4dsa12$$$YS" * 10
    query = job.query(long_invalid_query)
    with pytest.raises(Exception) as excinfo:
        query.run(rows_to_fetch=1)
    assert 'Token AERIOGFJ304TQ34PROJQWE was not valid.' in str(excinfo.value)
    
    # Test valid query with zero rows to fetch
    query = job.query("SELECT * FROM SAMPLE.employee")
    res = query.run(rows_to_fetch=0)
    assert res['data'] == [], "Expected empty result set when rows_to_fetch is 0"
    
    # Test valid query with non-numeric rows to fetch
    # use default rows_to_fetch == 100
    query = job.query("select * from sample.department")
    res = query.run(rows_to_fetch='s')
    assert res['success']
    
    # Test valid query with negative rows to fetch
    query = job.query("select * from sample.department")
    res = query.run(rows_to_fetch=-1)
    assert res['data'] == [], "Expected empty result set when rows_to_fetch < 0"
    
    # query.close()
    job.close()
    
def test_drop_table():
    job = SQLJob()
    _ = job.connect(creds)
    query = job.query('drop table sample.delete if exists')
    res = query.run()
    assert res['has_results'] == False
    job.close()
    
def test_fetch_more():
    job = SQLJob()
    _ = job.connect(creds)
    query = job.query('select * from sample.employee')
    res = query.run(rows_to_fetch=5)
    while not res['is_done']:
        res = query.fetch_more(10)
        assert len(res['data']) > 0
    
    job.close()
    assert res['is_done']
    
def test_prepare_statement():
    job = SQLJob()
    _ = job.connect(creds)
    opts = QueryOptions(
        parameters=[500]
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    res = query.run()
    assert res['success']
    assert len(res['data']) >= 17
    

def test_prepare_statement_terse():
    job = SQLJob()
    _ = job.connect(creds)
    opts = QueryOptions(
        parameters=[500],
        isTerseResults=True
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    res = query.run()
    assert res['success']
    assert len(res['data']) >= 17
    assert 'metadata' in res
    
def test_prepare_statement_mult_params():
    job = SQLJob()
    _ = job.connect(creds)
    opts = QueryOptions(
        parameters=[500, 'PRES']
    )
    query = job.query('select * from sample.employee where bonus > ? and job = ?', opts=opts)
    res = query.run()
    assert res['success']
    job.close()
    

def test_prepare_statement_invalid_params():
    job = SQLJob()
    _ = job.connect(creds)
    opts = QueryOptions(
        parameters=['jjfkdsajf']
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    with pytest.raises(Exception) as execinfo:
        res = query.run()
    assert 'Data type mismatch. (Infinite or NaN)' in str(execinfo.value)
    
    
def test_prepare_statement_no_param():
    job = SQLJob()
    _ = job.connect(creds)
    opts = QueryOptions(
        parameters=[],
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    with pytest.raises(Exception) as execinfo:
        res = query.run()
    assert 'The number of parameter values set or registered does not match the number of parameters.' in str(execinfo.value)
    job.close()
    
def test_prepare_statement_too_many():
    job = SQLJob()
    _ = job.connect(creds)
    opts = QueryOptions(
        parameters=[500, 'hello']
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    with pytest.raises(Exception) as execinfo:
        res = query.run()
    assert 'Descriptor index not valid. (2>1)' in str(execinfo.value)
    
def test_prepare_statement_invalid_data():
    job = SQLJob()
    _ = job.connect(creds)
    opts = QueryOptions(
        parameters=[{'bonus': 500}]
    )
    query = job.query('select * from sample.employee where bonus > ?', opts=opts)
    with pytest.raises(Exception) as execinfo:
        res = query.run()
    assert 'JsonObject' in str(execinfo.value)
    
def test_run_from_job():
    job = SQLJob()
    _ = job.connect(creds)
    res = job.query_and_run('select * from sample.employee')
    assert res['success'] 
    
def test_multiple_statements():
    job = SQLJob()
    _ = job.connect(creds)

    resA = job.query("select * from sample.department").run()
    assert resA["success"] is True

    resB = job.query("select * from sample.employee").run()
    assert resB["success"] is True

    job.close()
    