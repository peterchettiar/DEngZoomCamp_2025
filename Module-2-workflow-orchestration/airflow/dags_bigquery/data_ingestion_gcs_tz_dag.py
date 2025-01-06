"""
A simple script to use airflow as an orchestration tool to Extract, 
Transform and Load data into BigQuery
"""

import os
from datetime import datetime
import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
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


# make directory in local airflow directory to store all the downloaded raw files
if not os.path.exists(f"{AIRFLOW_HOME}/raw_data"):
    os.makedirs(f"{AIRFLOW_HOME}/raw_data")

RAW_FILE_TEMPLATE = AIRFLOW_HOME + "/raw_data/taxi_zone_lookup.csv"

# similarly we can create a folder in the airflow directory to hold the processed files
if not os.path.exists(f"{AIRFLOW_HOME}/cleaned_data"):
    os.makedirs(f"{AIRFLOW_HOME}/cleaned_data")

CLEAN_FILE_TEMPLATE = AIRFLOW_HOME + "/cleaned_data/taxi_zone_lookup.parquet"

TABLE_NAME_TEMPLATE = "taxi_zone_lookup_table"


# Defining a helper function
def __clean_file(raw_file_name, clean_file_name):
    """helper function to do some basic cleaning on file"""
    # Read the Parquet file into a DataFrame
    df = pd.read_csv(raw_file_name)

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
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_tz_dag",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["dtc-de-homework"],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv > {RAW_FILE_TEMPLATE}",
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET,
            "raw_file_name": f"{RAW_FILE_TEMPLATE}",
            "destination_blob_name": f"{TABLE_NAME_TEMPLATE}.parquet",
            "clean_file_name": f"{CLEAN_FILE_TEMPLATE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        destination_project_dataset_table=f"{BQ_DATASET}.taxi_zone_lookup",
        bucket=BUCKET,
        source_objects=[f"{TABLE_NAME_TEMPLATE}.parquet"],
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
