from __future__ import annotations

import logging

from ray.data import Datasource
from ray.data.datasource import Reader

from data.snowflake.reader import _SnowflakeDatasourceReader

logger = logging.getLogger(__name__)


class SnowflakeDatasource(Datasource):
    def __init__(self, connection_args: dict, query: str):
        self._connection_args = connection_args
        self._query = query

    def create_reader(self, **read_args) -> Reader:
        return _SnowflakeDatasourceReader(
            connection_args=self._connection_args,
            query=self._query
        )
