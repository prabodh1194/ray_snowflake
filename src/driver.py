from __future__ import annotations

import logging

import pyarrow
import ray
from pyarrow import Table

import settings
from data.snowflake.datasource import SnowflakeDatasource

ray_data_logger = logging.getLogger("ray.data")

ray.init(logging_level=logging.DEBUG)

snowflake_datasource = SnowflakeDatasource(connection_args=settings.SNOWFLAKE_CONNECTION_PROPS, query="SELECT * FROM LINEITEM")

rds = ray.data.read_datasource(
    snowflake_datasource
)


def _comp(_ds: Table):
    return _ds.append_column(
        "L_QUANTITY_2",
        pyarrow.compute.multiply(_ds.column("L_QUANTITY"), 2)
    )


_ds = rds.map_batches(_comp, batch_format="pyarrow")


_l_q_2 = _ds.select_columns(["L_QUANTITY_2"]).materialize()

ray_data_logger.info("counting %s", _l_q_2.count())
ray_data_logger.info("summing %s", _l_q_2.sum())
