"""
Optimized connection pool implementation with O(1) job selection and efficient resource management.

This module provides significant performance improvements over the original pool implementation:
- O(1) ready job selection using deques instead of O(n) linear search
- Efficient load balancing without expensive sorting operations
- Cached job metrics to avoid repeated calculations
- Pre-warmed connections for better response times
"""

import asyncio
import heapq
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Set
from weakref import WeakSet

from ..data_types import DaemonServer, JDBCOptions, JobStatus, QueryOptions
from .pool_job import PoolJob

__all__ = ["OptimizedPool", "PoolOptions", "PoolAddOptions"]


@dataclass
class PoolOptions:
    creds: Optional[Union[DaemonServer, Dict[str, Any], Path]]
    max_size: int
    starting_size: int
    opts: Optional[JDBCOptions] = None
    section: Optional[str] = None
    pre_warm_connections: bool = True  # New: Pre-warm connections on init
    health_check_interval: float = 30.0  # New: Health check interval in seconds


@dataclass
class PoolAddOptions:
    existing_job: Optional[PoolJob] = None
    pool_ignore: Optional[bool] = None


@dataclass
class JobMetrics:
    """Cached job metrics to avoid repeated calculations."""
    running_count: int
    last_updated: float
    
    def is_stale(self, max_age: float = 1.0) -> bool:
        """Check if metrics are stale and need refresh."""
        import time
        return (time.time() - self.last_updated) > max_age


class OptimizedPool:
    """
    High-performance connection pool with O(1) job selection and efficient resource management.
    
    Key optimizations:
    - Ready jobs maintained in O(1) deque for instant access
    - Busy jobs tracked in min-heap for efficient load balancing
    - Cached job metrics to avoid repeated status checks
    - Pre-warmed connections for faster response times
    - Automatic cleanup of invalid connections
    """
    
    def __init__(self, options: PoolOptions) -> None:
        self.options = options
        
        # Core data structures - optimized for O(1) operations
        self.all_jobs: List[PoolJob] = []  # All jobs for lifecycle management
        self.ready_jobs: deque[PoolJob] = deque()  # O(1) ready job access
        self.busy_jobs_heap: List[tuple] = []  # Min-heap: (running_count, job_id, job)
        self.invalid_jobs: WeakSet[PoolJob] = WeakSet()  # Auto-cleanup on GC
        
        # Performance optimization caches
        self.job_metrics: Dict[str, JobMetrics] = {}  # Cached job metrics
        self.job_id_to_job: Dict[str, PoolJob] = {}  # O(1) job lookup by ID
        
        # Async coordination
        self._job_creation_lock = asyncio.Lock()  # Prevent concurrent job creation
        self._metrics_update_lock = asyncio.Lock()  # Thread-safe metrics updates
        self._cleanup_task: Optional[asyncio.Task] = None
        
        # Statistics for monitoring
        self.stats = {
            "jobs_created": 0,
            "jobs_destroyed": 0,
            "ready_queue_hits": 0,
            "busy_job_selections": 0,
            "cache_hits": 0,
            "cache_misses": 0
        }

    async def init(self):
        """Initialize pool with optimized startup sequence."""
        if self.options.max_size <= 0:
            raise ValueError("Max size must be greater than 0")
        elif self.options.starting_size <= 0:
            raise ValueError("Starting size must be greater than 0")
        elif self.options.starting_size > self.options.max_size:
            raise ValueError("Max size must be greater than or equal to starting size")

        # Pre-warm connections concurrently for faster startup
        if self.options.pre_warm_connections:
            await self._pre_warm_connections()
        else:
            # Standard sequential initialization
            for _ in range(self.options.starting_size):
                await self._add_job_optimized()
        
        # Start background cleanup task
        if self.options.health_check_interval > 0:
            self._cleanup_task = asyncio.create_task(self._background_cleanup())

    async def _pre_warm_connections(self):
        """Pre-warm connections concurrently for faster pool initialization."""
        tasks = []
        for _ in range(self.options.starting_size):
            task = asyncio.create_task(self._add_job_optimized())
            tasks.append(task)
        
        # Wait for all connections to be established
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _background_cleanup(self):
        """Background task for periodic cleanup and health checks."""
        while True:
            try:
                await asyncio.sleep(self.options.health_check_interval)
                await self._cleanup_invalid_jobs()
                await self._refresh_stale_metrics()
            except asyncio.CancelledError:
                break
            except Exception as e:
                # Log error but continue cleanup cycle
                print(f"Background cleanup error: {e}")

    async def __aenter__(self):
        await self.init()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.end()

    def __str__(self):
        """Enhanced string representation with performance metrics."""
        ready_count = len(self.ready_jobs)
        busy_count = len(self.busy_jobs_heap)
        total_jobs = len(self.all_jobs)
        invalid_count = len(self.invalid_jobs)
        
        return (
            f"OptimizedPool Stats:\n"
            f"  Total Jobs: {total_jobs} (Ready: {ready_count}, Busy: {busy_count}, Invalid: {invalid_count})\n"
            f"  Performance: Ready hits: {self.stats['ready_queue_hits']}, "
            f"Cache hit ratio: {self._calculate_cache_hit_ratio():.2%}\n"
            f"  Lifecycle: Created: {self.stats['jobs_created']}, Destroyed: {self.stats['jobs_destroyed']}"
        )

    def _calculate_cache_hit_ratio(self) -> float:
        """Calculate cache hit ratio for monitoring."""
        total_requests = self.stats["cache_hits"] + self.stats["cache_misses"]
        return self.stats["cache_hits"] / total_requests if total_requests > 0 else 0.0

    def has_space(self) -> bool:
        """O(1) check if pool has space for new jobs."""
        valid_job_count = len(self.all_jobs) - len(self.invalid_jobs)
        return valid_job_count < self.options.max_size

    def get_active_job_count(self) -> int:
        """O(1) get count of active jobs."""
        return len(self.ready_jobs) + len(self.busy_jobs_heap)

    async def _add_job_optimized(
        self, options: PoolAddOptions = PoolAddOptions(existing_job=None, pool_ignore=None)
    ) -> PoolJob:
        """Optimized job creation with efficient tracking."""
        async with self._job_creation_lock:  # Prevent concurrent job creation
            if options.existing_job:
                await self._cleanup_invalid_jobs()

            new_job: PoolJob = options.existing_job or PoolJob(self.options.opts)

            if not options.pool_ignore:
                self.all_jobs.append(new_job)
                self.job_id_to_job[new_job.get_unique_id()] = new_job

            if new_job.get_status() == JobStatus.NotStarted:
                await new_job.connect(self.options.creds, section=self.options.section)
            
            # Add to ready queue if job is ready
            if new_job.get_status() == JobStatus.Ready:
                self.ready_jobs.append(new_job)
                self._update_job_metrics(new_job)
            
            self.stats["jobs_created"] += 1
            return new_job

    def _update_job_metrics(self, job: PoolJob) -> None:
        """Update cached job metrics efficiently."""
        import time
        job_id = job.get_unique_id()
        running_count = job.get_running_count()
        
        self.job_metrics[job_id] = JobMetrics(
            running_count=running_count,
            last_updated=time.time()
        )

    def _get_cached_running_count(self, job: PoolJob) -> int:
        """Get running count with caching to avoid repeated calculations."""
        job_id = job.get_unique_id()
        metrics = self.job_metrics.get(job_id)
        
        if metrics and not metrics.is_stale():
            self.stats["cache_hits"] += 1
            return metrics.running_count
        
        # Cache miss - update metrics
        self.stats["cache_misses"] += 1
        running_count = job.get_running_count()
        self._update_job_metrics(job)
        return running_count

    async def get_job_optimized(self) -> PoolJob:
        """
        O(1) optimized job selection using ready queue and efficient load balancing.
        
        Returns:
            PoolJob: Available job for query execution
        """
        # O(1) check for ready jobs
        if self.ready_jobs:
            job = self.ready_jobs.popleft()
            self.stats["ready_queue_hits"] += 1
            
            # Move to busy heap with current load
            running_count = self._get_cached_running_count(job)
            heapq.heappush(self.busy_jobs_heap, (running_count, id(job), job))
            
            return job
        
        # No ready jobs - find least busy job
        await self._refresh_busy_jobs_heap()
        
        if self.busy_jobs_heap:
            # Get least busy job (O(1) from min-heap)
            running_count, job_id, least_busy_job = self.busy_jobs_heap[0]
            
            # Check if we should create a new job instead of overloading
            if self.has_space() and running_count > 2:
                return await self._add_job_optimized()
            
            self.stats["busy_job_selections"] += 1
            return least_busy_job
        
        # No jobs available - create new one if possible
        if self.has_space():
            return await self._add_job_optimized()
        
        raise RuntimeError("No jobs available and pool is at maximum capacity")

    async def _refresh_busy_jobs_heap(self) -> None:
        """Efficiently refresh the busy jobs heap with current load information."""
        # Rebuild heap with current running counts
        current_busy_jobs = []
        
        for running_count, job_id, job in self.busy_jobs_heap:
            if job.get_status() == JobStatus.Busy:
                current_running_count = self._get_cached_running_count(job)
                current_busy_jobs.append((current_running_count, id(job), job))
            elif job.get_status() == JobStatus.Ready:
                # Move back to ready queue
                self.ready_jobs.append(job)
        
        # Replace heap with updated values
        self.busy_jobs_heap = current_busy_jobs
        heapq.heapify(self.busy_jobs_heap)

    async def _refresh_stale_metrics(self) -> None:
        """Refresh stale cached metrics in background."""
        async with self._metrics_update_lock:
            stale_job_ids = []
            
            for job_id, metrics in self.job_metrics.items():
                if metrics.is_stale():
                    stale_job_ids.append(job_id)
            
            # Update stale metrics
            for job_id in stale_job_ids:
                job = self.job_id_to_job.get(job_id)
                if job:
                    self._update_job_metrics(job)

    async def _cleanup_invalid_jobs(self) -> None:
        """Efficiently cleanup invalid/ended jobs."""
        INVALID_STATES = [JobStatus.Ended, JobStatus.NotStarted]
        
        # Remove invalid jobs from all data structures
        valid_jobs = []
        for job in self.all_jobs:
            if job.get_status() in INVALID_STATES:
                self.invalid_jobs.add(job)
                job_id = job.get_unique_id()
                
                # Cleanup from all tracking structures
                self.job_id_to_job.pop(job_id, None)
                self.job_metrics.pop(job_id, None)
                self.stats["jobs_destroyed"] += 1
            else:
                valid_jobs.append(job)
        
        self.all_jobs = valid_jobs
        
        # Clean up ready queue
        valid_ready = deque()
        while self.ready_jobs:
            job = self.ready_jobs.popleft()
            if job not in self.invalid_jobs:
                valid_ready.append(job)
        self.ready_jobs = valid_ready
        
        # Clean up busy heap
        valid_busy = [(count, job_id, job) for count, job_id, job in self.busy_jobs_heap 
                      if job not in self.invalid_jobs]
        self.busy_jobs_heap = valid_busy
        heapq.heapify(self.busy_jobs_heap)

    # Public API methods with optimized implementations
    async def get_job(self) -> PoolJob:
        """Public API: Get an available job for query execution."""
        return await self.get_job_optimized()

    async def wait_for_job(self, use_new_job: bool = False) -> PoolJob:
        """Wait for an available job, optionally creating a new one."""
        if use_new_job and self.has_space():
            return await self._add_job_optimized()
        return await self.get_job_optimized()

    async def pop_job(self) -> PoolJob:
        """Remove and return a job from the pool."""
        if self.ready_jobs:
            return self.ready_jobs.popleft()
        
        # Create temporary job not added to pool
        return await self._add_job_optimized(PoolAddOptions(pool_ignore=True))

    async def query(self, sql: str, opts: Union[QueryOptions, Dict[str, Any]] = None):
        """Execute a query using an available job."""
        job = await self.get_job_optimized()
        return job.query(sql, opts)

    async def execute(self, sql: str, opts: Union[QueryOptions, Dict[str, Any]] = None):
        """Execute a query and return results."""
        job = await self.get_job_optimized()
        return await job.query_and_run(sql, opts=opts)

    async def end(self):
        """Cleanup pool and close all connections."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Close all jobs
        for job in self.all_jobs:
            try:
                await job.close()
            except Exception:
                pass  # Ignore errors during cleanup
        
        # Clear all data structures
        self.all_jobs.clear()
        self.ready_jobs.clear()
        self.busy_jobs_heap.clear()
        self.job_metrics.clear()
        self.job_id_to_job.clear()