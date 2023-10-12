from __future__ import annotations

import logging
from collections import defaultdict

import ray
from pyarrow import Table

import settings
from data.snowflake.datasource import SnowflakeDatasource

logger = logging.getLogger(__name__)
ray_data_logger = logging.getLogger("ray.data")

ray.init(logging_level=logging.DEBUG, namespace="agg")

snowflake_datasource = SnowflakeDatasource(
    connection_args=settings.SNOWFLAKE_CONNECTION_PROPS,
    query="SELECT * exclude (timestamp) FROM TEST.PUBLIC.EVENTS_DATA limit 100000"
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
        self.value.add(event_name)
        return self.value

    def get_total_events(self) -> set[str]:
        return self.value


@ray.remote
class Counter:
    def __init__(self):
        self.value = defaultdict(lambda: defaultdict(int))

    def increment(self, *, event_name: str, user_id: str, by: int) -> dict[str, dict[str, int]]:
        self.value[user_id][event_name] += by
        return self.value

    def get_counter(self) -> dict[str, dict[str, int]]:
        return dict(self.value)


tot_events = TotalEvents.options(name="evt", get_if_exists=True).remote()


def _count(_ds: Table):
    _gs = _ds.group_by(["EVENT_NAME", "USER_ID"]).aggregate([("EVENT_NAME", "count")])

    for i in range(_gs.num_rows):
        event_name = _gs[1][i]
        user_id = _gs[2][i]
        inc = _gs[0][i]

        _k = event_name.as_py()

        ctr = Counter.options(name=_k[0], get_if_exists=True, lifetime="detached").remote()
        ctr.increment.remote(event_name=_k, user_id=user_id.as_py(), by=inc.as_py())

        tot_events.add.remote(event_name=_k)

    return _ds


rds.map_batches(_count, batch_format="pyarrow").materialize()

ray_data_logger.info(ray.get(tot_events.get_total_events.remote()))

for k in ray.get(tot_events.get_total_events.remote()):
    _act = ray.get_actor(k[0], "agg")
    ray_data_logger.info(ray.get(_act.get_counter.remote()))
