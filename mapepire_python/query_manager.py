from typing import Any, Dict, Optional, Union

from .client.query import Query
from .client.sql_job import SQLJob
from .data_types import QueryOptions
from .pool.pool_job import PoolJob
from .pool.pool_query import PoolQuery


class QueryManager:
    def __init__(self, job: Union[SQLJob, PoolJob]) -> None:
        self.job = job

    def get_query_options(
        self, opts: Optional[Union[Dict[str, Any], QueryOptions]] = None
    ) -> QueryOptions:
        query_options = (
            opts
            if isinstance(opts, QueryOptions)
            else (
                QueryOptions(**opts)
                if isinstance(opts, dict)
                else QueryOptions(isClCommand=False, parameters=None, autoClose=False)
            )
        )

        return query_options

    def create_query(
        self,
        query: str,
        opts: Optional[Union[Dict[str, Any], QueryOptions]] = None,
    ) -> Union[Query, PoolQuery]:
        if opts and not isinstance(opts, (dict, QueryOptions)):
            raise Exception("opts must be a dictionary, a QueryOptions object, or None")

        query_options = self.get_query_options(opts)

        try:
            return (
                Query(self.job, query, opts=query_options)
                if isinstance(self.job, SQLJob)
                else PoolQuery(self.job, query=query, opts=query_options)
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create query: {e}")

    def run_query(self, query: Query, rows_to_fetch: Optional[int] = None) -> Dict[str, Any]:
        return query.run(rows_to_fetch=rows_to_fetch)

    def query_and_run(
        self, query: str, opts: Optional[Union[Dict[str, Any], QueryOptions]] = None, **kwargs
    ) -> Dict[str, Any]:
        try:
            with self.create_query(query, opts) as query:  # type: ignore
                return query.run(**kwargs)  # type: ignore
        except Exception as e:
            raise RuntimeError(f"Failed to run query: {e}")

    async def query_and_run_async(
        self, query: str, opts: Optional[Union[Dict[str, Any], QueryOptions]] = None, **kwargs
    ) -> Dict[str, Any]:
        try:
            async with self.create_query(query, opts) as query:  # type: ignore
                return await query.run(**kwargs)  # type: ignore
        except Exception as e:
            raise RuntimeError(f"Failed to run query: {e}")
