from .pool_client import Pool as OriginalPool
from .optimized_pool_client import OptimizedPool, PoolOptions
from .pool_job import PoolJob

# Use optimized pool by default, but keep original available
Pool = OptimizedPool

__all__ = ["Pool", "OptimizedPool", "OriginalPool", "PoolJob", "PoolOptions"]
