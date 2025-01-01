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
URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"
URL_TEMPLATE = (
    URL_PREFIX + "/fhv_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv.gz"
)

# make directory in local airflow directory to store all the downloaded raw files
if not os.path.exists(f"{AIRFLOW_HOME}/raw_data"):
    os.makedirs(f"{AIRFLOW_HOME}/raw_data")

RAW_FILE_TEMPLATE = (
    AIRFLOW_HOME
    + "/raw_data/fhvtaxi_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv"
)

# similarly we can create a folder in the airflow directory to hold the processed files
if not os.path.exists(f"{AIRFLOW_HOME}/cleaned_data"):
    os.makedirs(f"{AIRFLOW_HOME}/cleaned_data")

CLEAN_FILE_TEMPLATE = (
    AIRFLOW_HOME
    + "/cleaned_data/fhvtaxi_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
)

TABLE_NAME_TEMPLATE = "fhv_taxi_{{ execution_date.strftime('%Y_%m') }}"


# Defining a helper function
def __clean_file(raw_file_name, clean_file_name):
    """helper function to do some basic cleaning on file"""

    try:
        # Read the Parquet file into a DataFrame
        df = pd.read_csv(raw_file_name, low_memory=True)
    except Exception as e:
        # Read the Parquet file into a DataFrame
        print(f"Error reading file with default encoding: {e}")
        df = pd.read_csv(raw_file_name, encoding="unicode_escape")

    df.columns = [col.lower() for col in df.columns]

    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])

    # changing the dtypes of specific columns
    df["pulocationid"] = df["pulocationid"].astype("Int64", errors="ignore")
    df["dolocationid"] = df["dolocationid"].astype("Int64", errors="ignore")
    df["sr_flag"] = df["sr_flag"].astype("Int64", errors="ignore")

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
    "end_date": datetime(2020, 1, 1),
    "retries": 2,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag_hw_q3",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=["dtc-de-homework"],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {URL_TEMPLATE} > {RAW_FILE_TEMPLATE}.gz && gunzip {RAW_FILE_TEMPLATE}.gz",
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET,
            "raw_file_name": f"{RAW_FILE_TEMPLATE}",
            "destination_blob_name": f"fhvtaxi_tripdata/{TABLE_NAME_TEMPLATE}.parquet",
            "clean_file_name": f"{CLEAN_FILE_TEMPLATE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        destination_project_dataset_table=f"{BQ_DATASET}.fhvtaxi_trips",
        bucket=BUCKET,
        source_objects=["fhvtaxi_tripdata/*.parquet"],
        source_format="PARQUET",
    )

    remove_files_task = BashOperator(
        task_id="remove_files_task",
        bash_command=f"rm  {RAW_FILE_TEMPLATE} {CLEAN_FILE_TEMPLATE}",
    )

(
    download_dataset_task
    >> local_to_gcs_task
    >> bigquery_external_table_task
    >> remove_files_task
)
