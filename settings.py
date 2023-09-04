from __future__ import annotations

import logging
import os

import dotenv

logger = logging.getLogger(__name__)

dotenv.load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')


SNOWFLAKE_CONNECTION_PROPS = {
    'database': 'SNOWFLAKE_SAMPLE_DATA',
    'schema': 'TPCH_SF100',
    'warehouse': 'COMPUTE_WH',
    'user': SNOWFLAKE_USER,
    'password': SNOWFLAKE_PASSWORD,
    'account': SNOWFLAKE_ACCOUNT,
}
