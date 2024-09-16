from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ..data_types import DaemonServer, JDBCOptions, JobStatus, QueryOptions
from .pool_job import PoolJob

__all__ = ["Pool"]


@dataclass
class PoolOptions:
    creds: Optional[Union[DaemonServer, Dict[str, Any], Path]]
    max_size: int
    starting_size: int
    opts: Optional[JDBCOptions] = None
    section: Optional[str] = None


@dataclass
class PoolAddOptions:
    existing_job: Optional[PoolJob] = None
    pool_ignore: Optional[bool] = None


INVALID_STATES = [JobStatus.Ended, JobStatus.NotStarted]


class Pool:
    def __init__(self, options: PoolOptions) -> None:
        self.options = options
        self.jobs: List[PoolJob] = []

    async def init(self):
        if self.options.max_size <= 0:
            raise ValueError("Max size must be greater than 0")
        elif self.options.starting_size <= 0:
            raise ValueError("Starting size must be greater than 0")
        elif self.options.starting_size > self.options.max_size:
            raise ValueError("Max size must be greater than or equal to starting size")

        for _ in range(self.options.starting_size):
            await self._add_job()

    async def __aenter__(self):
        await self.init()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.end()

    def __str__(self):
        job_details = []
        active_jobs = 0

        for job in self.jobs:
            status = job.get_status()
            running_count = job.get_running_count()
            unique_id = job.get_unique_id()
            job_details.append(
                f"Job ID: {unique_id}, Status: {status}, Running Count: {running_count}"
            )
            if status not in INVALID_STATES:
                active_jobs += 1

        job_details_str = "\n".join(job_details)
        return f"-------\nJobs:\n{job_details_str}\nActive Jobs: {active_jobs}\n-------"

    def has_space(self):
        return (
            len([j for j in self.jobs if j.get_status() not in INVALID_STATES])
            < self.options.max_size
        )

    def get_active_job_count(self):
        return len([j for j in self.jobs if j.get_status() in {JobStatus.Busy, JobStatus.Ready}])

    def cleanup(self):
        for i in range(len(self.jobs) - 1, -1, -1):
            if self.jobs[i].get_status() in INVALID_STATES:
                self.jobs.pop(i)

    async def _add_job(
        self, options: PoolAddOptions = PoolAddOptions(existing_job=None, pool_ignore=None)
    ):
        if options.existing_job:
            self.cleanup()

        new_sql_job: PoolJob = options.existing_job or PoolJob(self.options.opts)

        if not options.pool_ignore:
            self.jobs.append(new_sql_job)

        if new_sql_job.get_status() == JobStatus.NotStarted:
            await new_sql_job.connect(self.options.creds, section=self.options.section)

        return new_sql_job

    def _get_ready_job(self) -> PoolJob:
        return next((job for job in self.jobs if job.get_status() == JobStatus.Ready), None)

    def _get_ready_job_idx(self):
        return next(
            (index for index, job in enumerate(self.jobs) if job.get_status() == JobStatus.Ready),
            -1,
        )

    async def get_job(self):
        job = self._get_ready_job()
        if not job:
            busy_jobs: PoolJob = [j for j in self.jobs if j.get_status() == JobStatus.Busy]
            freeist: PoolJob = sorted(busy_jobs, key=lambda job: job.get_running_count())[0]
            if self.has_space() and freeist.get_running_count() > 2:
                await self._add_job()
            return freeist
        return job

    async def wait_for_job(self, use_new_job: bool = False):
        job = self._get_ready_job()
        if not job:
            if self.has_space() or use_new_job:
                new_job = await self._add_job()
                return new_job
            else:
                return await self.get_job()
        return job

    async def pop_job(self):
        index = self._get_ready_job_idx()
        if index > -1:
            return self.jobs.pop(index)
        new_job = await self._add_job(PoolAddOptions(pool_ignore=True))
        return new_job

    async def query(self, sql: str, opts: Union[QueryOptions, Dict[str, Any]] = None):
        job = await self.get_job()
        return job.query(sql, opts)

    async def execute(self, sql: str, opts: Union[QueryOptions, Dict[str, Any]] = None):
        job = await self.get_job()
        return await job.query_and_run(sql, opts=opts)

    async def end(self):
        for j in self.jobs:
            await j.close()
