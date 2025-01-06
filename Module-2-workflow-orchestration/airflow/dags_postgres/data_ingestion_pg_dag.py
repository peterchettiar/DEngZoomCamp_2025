"""
Data ingestion DAG for downloading NYC taxi trip data from S3, transforming it, and ingesting it into PostgreSQL.
"""

import os
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_script_pg import ingest_callable

# environment variable - if variable is set then "AIRFLOW HOME" else "/opt/airflow/"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")


# templating to parameterise the filename
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_TEMPLATE = (
    URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
)
OUTPUT_FILE_TEMPLATE = (
    AIRFLOW_HOME + "/yellowtaxi_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
)
TABLE_NAME_TEMPLATE = "yellow_taxi_{{ execution_date.strftime('%Y_%m') }}"

# DAG default arguments
default_args = {
    "start_date": datetime(2021, 1, 1),
    "end_date": datetime(2024, 2, 1),
    "retries": 3,
}


@dag(
    default_args=default_args,
    schedule_interval="0 6 2 * *",
)
def ingest_data_postgres():
    """
    DAG generator function to run our tasks
    """
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}",
    )

    ingestion_task = PythonOperator(
        task_id="ingest_to_postgres_task",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            parquet_file=OUTPUT_FILE_TEMPLATE,
        ),
    )

    # define task dependencies
    download_dataset_task >> ingestion_task


DAG = ingest_data_postgres()
