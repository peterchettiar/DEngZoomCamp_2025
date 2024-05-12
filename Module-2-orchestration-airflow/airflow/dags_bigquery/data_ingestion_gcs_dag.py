"""
A simple script to use airflow as an orchestration tool to Extract, 
Transform and Load data into BigQuery
"""

import os
from datetime import datetime
import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)

# environment variables - if variable is set then "AIRFLOW HOME" else "/opt/airflow/"
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET = os.getenv("GCP_GCS_BUCKET", "test_bucket_415106")
BQ_DATASET = os.getenv("BQ_DATASET", "test_dataset")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow/")


# templating to parameterise the filename
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_TEMPLATE = (
    URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
)

# make directory in local airflow directory to store all the downloaded raw files
if not os.path.exists(f"{AIRFLOW_HOME}/raw_data"):
    os.makedirs(f"{AIRFLOW_HOME}/raw_data")

RAW_FILE_TEMPLATE = (
    AIRFLOW_HOME
    + "/raw_data/yellowtaxi_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
)

# similarly we can create a folder in the airflow directory to hold the processed files
if not os.path.exists(f"{AIRFLOW_HOME}/cleaned_data"):
    os.makedirs(f"{AIRFLOW_HOME}/cleaned_data")

CLEAN_FILE_TEMPLATE = (
    AIRFLOW_HOME
    + "/cleaned_data/yellowtaxi_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
)

TABLE_NAME_TEMPLATE = "yellow_taxi_{{ execution_date.strftime('%Y_%m') }}"


# Defining a helper function
def __clean_file(raw_file_name, clean_file_name):
    """helper function to do some basic cleaning on file"""
    # Read the Parquet file into a DataFrame
    df = pd.read_parquet(raw_file_name)
    # Fill NaN values with 0 and convert specific columns to int64 data type
    df = df.fillna(0).astype(
        {
            "passenger_count": "int64",
            "RatecodeID": "int64",
            "store_and_fwd_flag": "bytes",
        }
    )
    # Save the processed DataFrame as a Parquet file
    df.to_parquet(clean_file_name, index=False)


# Define the function
def upload_to_gcs(bucket_name, raw_file_name, destination_blob_name, clean_file_name):
    """Uploads a file to the bucket"""
    # Create a storage client
    storage_client = storage.Client()
    # Get the specified bucket
    bucket = storage_client.bucket(bucket_name)
    # Create a blob object with the specified destination blob name
    blob = bucket.blob(destination_blob_name)

    # running helper function to clean raw file and move to clean file folder
    __clean_file(raw_file_name, clean_file_name)

    # Upload the cleaned Parquet file to the specified destination blob
    blob.upload_from_filename(clean_file_name)


default_args = {
    "start_date": datetime(2021, 1, 1),
    "end_date": datetime(2024, 2, 1),
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ingest_data_bigquery",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {RAW_FILE_TEMPLATE}",
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET,
            "raw_file_name": f"{RAW_FILE_TEMPLATE}",
            "destination_blob_name": f"yellowtaxi_tripdata/{TABLE_NAME_TEMPLATE}.parquet",
            "clean_file_name": f"{CLEAN_FILE_TEMPLATE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        destination_project_dataset_table=f"{BQ_DATASET}.yellowtaxi_trips",
        bucket=BUCKET,
        source_objects=["yellowtaxi_tripdata/*.parquet"],
        source_format="PARQUET",
    )


download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task
