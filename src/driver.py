from __future__ import annotations

import logging

import ray

import settings
from data.snowflake.datasource import SnowflakeDatasource

ray_data_logger = logging.getLogger("ray.data")

ray.init(logging_level=logging.DEBUG)

rds = ray.data.read_datasource(
    SnowflakeDatasource(
        connection_args=settings.SNOWFLAKE_CONNECTION_PROPS,
        query="SELECT * FROM LINEITEM"
    ),
    parallelism=4
)

ds = rds.add_column(
    "L_QUANTITY_2", lambda _df: _df["L_QUANTITY"] * 2
).to_pandas()

ray_data_logger.info("pbd cols %s", ds.columns)
ray_data_logger.info("pd size %s", ds.size)
ray_data_logger.info("pd size %s", len(ds))
ray_data_logger.info("pd size %s", ds.shape)
ray_data_logger.info("pd show %s", ds.head())
