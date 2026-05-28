import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union

from ..async_base_job import AsyncBaseJob
from ..data_types import DaemonServer

__all__ = ["PoolJob"]

logger = logging.getLogger(__name__)


class PoolJob(AsyncBaseJob):
    """Native async SQL job for use within a connection pool.

    Extends AsyncBaseJob with tracing capabilities specific to pool usage.
    All WebSocket lifecycle, message routing, and query execution logic
    lives in AsyncBaseJob.
    """

    def __init__(
        self,
        creds: Optional[Union[DaemonServer, Dict[str, Any], Path]] = None,
        options: Optional[Dict[Any, Any]] = None,
        **kwargs,
    ) -> None:
        super().__init__(creds, options, **kwargs)
        self.trace_file = None
        self.is_tracing_channel_data = False
        self.enable_local_trace = False

    def enable_local_trace_data(self):
        self.enable_local_trace = True

    def enable_local_channel_trace(self):
        self.is_tracing_channel_data = True
