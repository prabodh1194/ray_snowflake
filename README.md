# ray_snowflake

This repo does a test drive of using snowflake as a source in ray. This has been done after I struggled to make -- [anyscale snowflake connector](https://github.com/anyscale/datasets-database/blob/master/python/ray/data/datasource/snowflake_datasource.py) work. I have written a test driver as well which shows how to do a basic transform on this dataset.

### Setup

You just need a valid snowflake account to test this out. I tested this code on a trial snowflake account and hence could get this to work with `ACCOUNTADMIN` creds.

1. `cp .env.example .env`
2. Open the `.env` file and setup the 3 creds.

### Run

Once you have filled up the creds correctly, you can run the script and see things happen. The test script performs a write to a DB as well. You can tinker the script to put in the correct db name.
