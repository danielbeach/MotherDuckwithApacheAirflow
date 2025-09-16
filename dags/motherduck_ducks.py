"""

DAG: Harddrive CSVs -> MotherDuck (pickles)
- Uses an existing MotherDuck secret named 's3'
- Loads CSVs from s3://confessions-of-a-data-guy/harddrivedata/
- Creates pickles.harddrive_raw and pickles.failure_by_date
"""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models import Variable
import duckdb

MD_TOKEN = ""                 # store securely in Airflow
S3_INPUT = "s3://confessions-of-a-data-guy/harddrivedata/*/*.csv"
MD_DB    = "pickles"

@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False, tags=["duckdb","motherduck"])
def harddrive_to_motherduck():
    @task
    def load_raw_from_s3():
        # Connect to MotherDuck database "pickles"
        con = duckdb.connect(f"md:{MD_DB}?motherduck_token={MD_TOKEN}")

        # Ensure DB and select it
        con.sql(f"CREATE DATABASE IF NOT EXISTS {MD_DB};")
        con.sql(f"USE {MD_DB};")

        # Read CSVs from S3 using the existing MD secret 's3' (no code needed here).
        # Create/replace the raw table with typed columns.
        con.sql(f"""
            CREATE OR REPLACE TABLE harddrive_raw AS
            SELECT
                TRY_CAST(date    AS DATE) AS "date",
                CAST(model       AS VARCHAR) AS model,
                TRY_CAST(failure AS INT)  AS failure
            FROM read_csv_auto('{S3_INPUT}', header=true, ignore_errors=true);
        """)

    @task
    def aggregate_failures_by_date():
        con = duckdb.connect(f"md:{MD_DB}?motherduck_token={MD_TOKEN}")
        con.sql(f"USE {MD_DB};")
        con.sql("""
            CREATE OR REPLACE TABLE failure_by_date AS
            SELECT "date", SUM(failure) AS failure_count
            FROM harddrive_raw
            GROUP BY "date"
            ORDER BY "date";
        """)

    load_raw_from_s3() >> aggregate_failures_by_date()

harddrive_to_motherduck()

