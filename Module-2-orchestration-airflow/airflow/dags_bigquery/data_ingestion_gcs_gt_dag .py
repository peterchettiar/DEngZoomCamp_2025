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
BUCKET = os.getenv("GCP_GCS_BUCKET")
BQ_DATASET = os.getenv("BQ_DATASET", "nyc_tlc_data")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow/")


# templating to parameterise the filename
URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green"
URL_TEMPLATE = (
    URL_PREFIX + "/green_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv.gz"
)

# make directory in local airflow directory to store all the downloaded raw files
if not os.path.exists(f"{AIRFLOW_HOME}/raw_data"):
    os.makedirs(f"{AIRFLOW_HOME}/raw_data")

RAW_FILE_TEMPLATE = (
    AIRFLOW_HOME
    + "/raw_data/greentaxi_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv"
)

# similarly we can create a folder in the airflow directory to hold the processed files
if not os.path.exists(f"{AIRFLOW_HOME}/cleaned_data"):
    os.makedirs(f"{AIRFLOW_HOME}/cleaned_data")

CLEAN_FILE_TEMPLATE = (
    AIRFLOW_HOME
    + "/cleaned_data/greentaxi_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
)

TABLE_NAME_TEMPLATE = "green_taxi_{{ execution_date.strftime('%Y_%m') }}"


# Defining a helper function
def __clean_file(raw_file_name, clean_file_name):
    """helper function to do some basic cleaning on file"""
    # Read the Parquet file into a DataFrame
    df = pd.read_csv(
        raw_file_name,
        parse_dates=["lpep_pickup_datetime", "lpep_dropoff_datetime"],
    )

    # changing the dtypes of specific columns
    df["VendorID"] = df["VendorID"].astype("Int64", errors="ignore")
    df["passenger_count"] = df["passenger_count"].astype("Int64", errors="ignore")
    df["RatecodeID"] = df["RatecodeID"].astype("Int64", errors="ignore")
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("str", errors="ignore")
    df["payment_type"] = df["payment_type"].astype("Int64", errors="ignore")
    df["trip_type"] = df["trip_type"].astype("Int64", errors="ignore")

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
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2021, 8, 1),
    "retries": 2,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ingest_data_bigquery_gt",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {RAW_FILE_TEMPLATE}.gz && gunzip {RAW_FILE_TEMPLATE}.gz",
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET,
            "raw_file_name": f"{RAW_FILE_TEMPLATE}",
            "destination_blob_name": f"greentaxi_tripdata/{TABLE_NAME_TEMPLATE}.parquet",
            "clean_file_name": f"{CLEAN_FILE_TEMPLATE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        destination_project_dataset_table=f"{BQ_DATASET}.greentaxi_trips",
        bucket=BUCKET,
        source_objects=["greentaxi_tripdata/*.parquet"],
        source_format="PARQUET",
    )


download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task
