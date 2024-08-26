from typing import Any, Dict, Optional, Union

from ..types import QueryOptions
from .query import Query
from .sql_job import SQLJob


class QueryManager:
    def __init__(self, job: SQLJob) -> None:
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
    ) -> Query:

        if opts and not isinstance(opts, (dict, QueryOptions)):
            raise Exception("opts must be a dictionary, a QueryOptions object, or None")

        query_options = self.get_query_options(opts)

        return Query(self.job, query, opts=query_options)

    def run_query(self, query: Query, rows_to_fetch: Optional[int] = None) -> Dict[str, Any]:
        return query.run(rows_to_fetch=rows_to_fetch)

    def query_and_run(
        self, query: str, opts: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Dict[str, Any]:
        with self.create_query(query, opts) as query:
            return query.run(**kwargs)
