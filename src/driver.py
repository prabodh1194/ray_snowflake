from __future__ import annotations

import logging

import pyarrow
import ray
from pyarrow import Table

import settings
from data.snowflake.datasource import SnowflakeDatasource

ray_data_logger = logging.getLogger("ray.data")

ray.init(logging_level=logging.DEBUG)

rds = ray.data.read_datasource(
    SnowflakeDatasource(
        connection_args=settings.SNOWFLAKE_CONNECTION_PROPS,
        query="SELECT * FROM LINEITEM"
    ),
    parallelism=3
)


def _comp(_ds: Table):
    return _ds.append_column(
        "L_QUANTITY_2",
        pyarrow.compute.multiply(_ds.column("L_QUANTITY"), 2)
    )


ds = rds.map_batches(_comp, batch_format="pyarrow").to_pandas()

ray_data_logger.info("pbd cols %s", ds.columns)
ray_data_logger.info("pd size %s", ds.size)
ray_data_logger.info("pd size %s", len(ds))
ray_data_logger.info("pd size %s", ds.shape)
ray_data_logger.info("pd show %s", ds.head())
