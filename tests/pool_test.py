# Fetch environment variables
import asyncio

import pytest
from mapepire_python.pool.pool_job import PoolJob
from mapepire_python.types import DaemonServer
import os

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

@pytest.mark.asyncio
async def test_pool():
    pool_job = PoolJob()
    
    result = await pool_job.connect(creds)
    print(result)
    

