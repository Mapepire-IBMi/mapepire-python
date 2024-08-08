# from dataclasses import dataclass
# from typing import List, Optional


# from python_wsdb.client.sql_job import SQLJob
# from python_wsdb.types import *


# @dataclass
# class PoolOptions:
#     creds: DaemonServer
#     opts: Optional[JDBCOptions]
#     max_size: int
#     starting_size: int


# class Pool:
#     def __init__(self, options: PoolOptions) -> None:
#         self.options = options
#         self.jobs: List[SQLJob] = []

#     def init(self):
#         ...

#     async def add_job(self):
#         new_job = SQLJob(self.options.opts)
#         self.jobs.append(new_job)
#         new_job.connect(self.options.creds)
#         return new_job
