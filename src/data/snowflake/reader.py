from __future__ import annotations

import logging
from typing import Optional, Iterable

import pyarrow as pa
from ray.data import ReadTask
from ray.data.block import BlockMetadata
from ray.data.datasource import Reader
from snowflake.connector import connect
from snowflake.connector.constants import FIELD_TYPES
from snowflake.connector.result_batch import ResultBatch

ray_data_logger = logging.getLogger("ray.data")

FIELD_TYPE_TO_PA_TYPE = [e.pa_type() for e in FIELD_TYPES]


class LazyReadTask(ReadTask):
    def __init__(self, arrow_batch: ResultBatch, metadata: BlockMetadata):
        self._arrow_batch = arrow_batch
        self._metadata = metadata

    def _read_fn(self) -> Iterable[pa.Table]:
        ray_data_logger.debug(
            "Reading %s rows from Snowflake", self._metadata.num_rows
        )
        return [self._arrow_batch.to_arrow()]


class _SnowflakeDatasourceReader(Reader):
    def __init__(self, connection_args: dict, query: str):
        self._connection_args = connection_args
        self._query = query

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(self, parallelism: int) -> list[ReadTask]:
        with connect(**self._connection_args) as conn:
            with conn.cursor() as cur:
                cur.execute(self._query)
                batches = cur.get_result_batches()

            read_tasks = []

            for batch in batches:
                metadata = BlockMetadata(
                    num_rows=batch.rowcount,
                    size_bytes=batch.uncompressed_size,
                    schema=pa.schema(
                        [
                            pa.field(
                                s.name,
                                FIELD_TYPE_TO_PA_TYPE[
                                    s.type_code
                                ]
                            )
                            for s in batch.schema
                        ]
                    ),
                    input_files=None,
                    exec_stats=None
                )

                _r_task = LazyReadTask(
                    arrow_batch=batch,
                    metadata=metadata
                )

                read_tasks.append(_r_task)

            return read_tasks
