from ..async_base_job import AsyncBaseJob

__all__ = ["AsyncSQLJob"]


class AsyncSQLJob(AsyncBaseJob):
    """Native async SQL job for standalone (non-pool) use.

    All WebSocket lifecycle, message routing, and query execution logic
    lives in AsyncBaseJob. This class exists as a named entry point for
    the PEP 249 async interface and direct use outside of a pool.
    """

    pass
