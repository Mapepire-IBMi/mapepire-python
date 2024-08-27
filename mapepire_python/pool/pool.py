# from dataclasses import dataclass
# from typing import List, Optional


# from mapepire_python.client.sql_job import SQLJob
# from mapepire_python.types import *


# @dataclass
# class PoolOptions:
#     creds: DaemonServer
#     opts: Optional[JDBCOptions]
#     max_size: int
#     starting_size: int


# @dataclass
# class PoolAddOptions:
#     existing_job: Optional[SQLJob]
#     pool_ignore: Optional[bool]


# INVALID_STATES = [JobStatus.Ended, JobStatus.NotStarted]


# class Pool:
#     def __init__(self, options: PoolOptions) -> None:
#         self.options = options
#         self.jobs: List[SQLJob] = []

#     def init(self): ...

#     def has_space(self):
#         return (
#             len([j for j in self.jobs if j.get_status() not in INVALID_STATES])
#             < self.options.max_size
#         )

#     def cleanup(self):
#         for i in range(len(self.jobs) - 1, -1, -1):
#             if self.jobs[i].get_status() in INVALID_STATES:
#                 del self.jobs[i]

#     def _add_job(self, options: PoolAddOptions):
#         if options.existing_job:
#             self.cleanup()

#         new_sql_job: SQLJob = options.existing_job or SQLJob(self.options.opts)

#         if options.pool_ignore is not True:
#             self.jobs.append(new_sql_job)

#         if new_sql_job.get_status() == JobStatus.NotStarted:
#             new_sql_job.connect(self.options.creds)

#         return new_sql_job

#     def _get_ready_job(self) -> SQLJob | None:
#         return next((job for job in self.jobs if job.get_status() == JobStatus.Ready), None)

#     def _get_ready_job_idx(self):
#         return next(
#             (index for index, job in enumerate(self.jobs) if job.get_status() == JobStatus.Ready),
#             None,
#         )
        
        
#     def get_job(self):
#         job = self._get_ready_job()
#         if job is None:
#             busy_jobs = [j for j in self.jobs if j.get_status() == JobStatus.Busy]
            
#             freeist = busy_jobs.
            
