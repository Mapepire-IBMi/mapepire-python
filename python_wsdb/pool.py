from dataclasses import dataclass, field
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from dataclasses_json import dataclass_json

from python_wsdb import *
from python_wsdb.client.sql_job import SQLJob


@dataclass
class PoolOptions:
    creds: DaemonServer
    opts: Optional[JDBCOptions]
    max_size: int
    starting_size: int
    
class Pool:
    def __init__(self, options: PoolOptions) -> None:
        self.options = options
        self.jobs: List[SQLJob] = []
    
    def init(self):
        ...
        
    async def add_job(self):
        new_job = SQLJob(self.options.opts)
        self.jobs.append(new_job)
        new_job.connect(self.options.creds)
        return new_job
        