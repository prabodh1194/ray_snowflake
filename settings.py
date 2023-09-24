from __future__ import annotations

import logging
import os

import dotenv

logger = logging.getLogger(__name__)

dotenv.load_dotenv()

SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
SNOWFLAKE_PASSWORD = os.environ['SNOWFLAKE_PASSWORD']


SNOWFLAKE_CONNECTION_PROPS = {
    'database': 'SNOWFLAKE_SAMPLE_DATA',
    'schema': 'TPCH_SF1',
    'warehouse': 'COMPUTE_WH',
    'user': SNOWFLAKE_USER,
    'password': SNOWFLAKE_PASSWORD,
    'account': SNOWFLAKE_ACCOUNT,
    'session_parameters': {
        'USE_CACHED_RESULT': False,
        'QUERY_TAG': 'ray-data'
    }
}
