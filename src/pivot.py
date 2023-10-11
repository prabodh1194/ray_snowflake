from __future__ import annotations

import logging

import ray
from pyarrow import Table

import settings
from data.snowflake.datasource import SnowflakeDatasource

logger = logging.getLogger(__name__)
ray_data_logger = logging.getLogger("ray.data")

ray.init(logging_level=logging.DEBUG, namespace="agg")

snowflake_datasource = SnowflakeDatasource(
    connection_args=settings.SNOWFLAKE_CONNECTION_PROPS,
    query="SELECT * exclude (timestamp) FROM TEST.PUBLIC.EVENTS_DATA limit 10000"
)

rds = ray.data.read_datasource(
    snowflake_datasource
)

ray_data_logger.info(rds.schema())


@ray.remote
class TotalEvents:
    def __init__(self):
        self.value = set()

    def add(self, event_name: str) -> set[str]:
        self.value.add(event_name.as_py())
        return self.value

    def get_total_events(self) -> set[str]:
        return self.value


@ray.remote
class Counter:
    def __init__(self):
        self.value = {}

    def increment(self, user_id: str) -> dict[str, int]:
        self.value[user_id] = self.value.get(user_id, 0) + 1
        return self.value

    def get_counter(self) -> dict[str, int]:
        return self.value


tot_events = TotalEvents.options(name="evt", get_if_exists=True).remote()


def _count(_ds: Table):
    for i in range(_ds.num_rows):
        event_name = _ds[0][i]
        user_id = _ds[-1][i]

        k = event_name.as_py()

        ctr = Counter.options(name=k, get_if_exists=True, lifetime="detached").remote()
        ctr.increment.remote(user_id=user_id.as_py())

        tot_events.add.remote(event_name=event_name)

    return _ds


rds.map_batches(_count, batch_format="pyarrow").materialize()

ray_data_logger.info(ray.get(tot_events.get_total_events.remote()))

for k in ray.get(tot_events.get_total_events.remote()):
    _act = ray.get_actor(k, "agg")
    ray_data_logger.info(ray.get(_act.get_counter.remote()))
    ray.kill(_act)
