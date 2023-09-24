from __future__ import annotations

import logging
from typing import Iterable

from ray.data import Datasource
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block
from ray.data.datasource import Reader, WriteResult
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

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

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
        **write_args,
    ) -> WriteResult:
        connection = connect(**self._connection_args)
        for block in blocks:
            block.to_pandas().pipe(
                lambda df: write_pandas(
                    connection,
                    df,
                    database=write_args["database"],
                    schema=write_args["schema"],
                    table_name=write_args["table_name"],
                    auto_create_table=write_args.get("auto_create_table"),
                    create_temp_table=write_args.get("create_temp_table"),
                )
            )
