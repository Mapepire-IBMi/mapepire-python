import os
import re

import pytest

from mapepire_python.client.sql_job import SQLJob
from mapepire_python.query_manager import QueryManager
from mapepire_python.data_types import DaemonServer, QueryOptions

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

def test_query_manager():
    # connection logic
    job = SQLJob()
    job.connect(creds)
    
    # Query Manager
    query_manager = QueryManager(job)
    
    # create a unique query
    query = query_manager.create_query("select * from sample.employee")
    
    # run query
    result = query_manager.run_query(query)
    
    assert result['success']
    job.close()
    with SQLJob(creds) as sql_job:
        query_manager = QueryManager(sql_job)
        with query_manager.create_query("select * from sample.employee") as query:
            result = query_manager.run_query(query, rows_to_fetch=1)
            print(result)
            

def test_query_manager_with_cm_q_and_run():
    with SQLJob(creds=creds) as job:
        query_manager = QueryManager(job)
        res = query_manager.query_and_run('select * from sample.employee')
        assert res['success'] == True
    

    
def test_context_manager():
    with SQLJob() as job:
        job.connect(creds)
    
        query_manager = QueryManager(job)
        query = query_manager.create_query("select * from sample.department")
        result = query_manager.run_query(query)
        assert result['success']

def test_simple_v2():
    with SQLJob(creds) as job:
        query_manager = QueryManager(job)
        query = query_manager.create_query('select * from sample.employee')
        result = query_manager.run_query(query, rows_to_fetch=5)
        assert result['success'] == True
        assert result['is_done'] == False
        assert result['has_results'] == True
        query.close()
        
def test_query_large_dataset():
    job = SQLJob()
    _ = job.connect(creds)
    query_manager = QueryManager(job)
    query = query_manager.create_query('select * from sample.employee')
    
    result = query_manager.run_query(query, rows_to_fetch=30)
    query.close()
    job.close()
    
    assert result['success'] == True
    assert result['is_done'] == False
    assert result['has_results'] == True
    assert len(result['data']) == 30
    
def test_query_and_run():
    with SQLJob(creds) as job:
        query_manager = QueryManager(job)
        result = query_manager.query_and_run('select * from sample.employee', rows_to_fetch=5)
        assert result['success'] == True
        assert result['is_done'] == False
        assert result['has_results'] == True