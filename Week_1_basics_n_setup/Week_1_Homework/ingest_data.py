"""
This script is meant for ingesting data into postgres

"""

import argparse
import time
import urllib.request
import gzip
from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm


def main(params):
    """Takes arguments and uploads local parquet files into postgres"""

    print("Starting data ingestion process...")

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db

    # record start time
    start_time = time.time()

    # download the compressed file for the green taxi dataset
    response = urllib.request.urlopen(params.green_taxi_url)

    # check if the request was successful
    if response.status == 200:
        # decompress the file
        with gzip.GzipFile(fileobj=BytesIO(response.read())) as f:
            # read the decompressed CSV file into pandas DataFrame
            green_taxi_df = pd.read_csv(f, low_memory=False)

    # read the parquet file
    taxi_zone_df = pd.read_csv(params.taxi_zones_url)

    # lets combine both dfs into a list
    df_list = [
        (params.first_table_name, green_taxi_df),
        (params.second_table_name, taxi_zone_df),
    ]

    # initialising an engine object and passing the database connection string as an argument
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # connect to database
    connection = engine.connect()

    for df in tqdm(df_list):
        # write dataframe to the database table
        print(f"Writing data to table {df[0]} in the database...")
        df[1].to_sql(name=df[0], con=connection, if_exists="replace", index=False)

    print("Data ingestion complete")

    # close the database connection
    connection.close()

    # Record end time
    end_time = time.time()

    # Calculate duration
    duration = end_time - start_time
    print(f"Data ingestion process completed in {duration:.2f} seconds.")


if __name__ == "__main__":

    # create an argument parser
    parser = argparse.ArgumentParser(
        description="Python script to ingest data into PostgresSQL database"
    )

    # Add arguments
    parser.add_argument("--user", type=str, help="Database username")
    parser.add_argument("--password", type=str, help="Database password")
    parser.add_argument("--host", type=str, help="Database host")
    parser.add_argument("--port", type=int, help="Database port")
    parser.add_argument("--db", type=str, help="Database name")
    parser.add_argument(
        "--first_table_name", type=str, help="First table name in the database"
    )
    parser.add_argument(
        "--second_table_name", type=str, help="Second table name in the database"
    )
    parser.add_argument(
        "--green_taxi_url",
        type=str,
        help="URL of the data file to ingest for green taxi",
    )
    parser.add_argument(
        "--taxi_zones_url",
        type=str,
        help="URL of the data file to ingest for taxi zone",
    )

    args = parser.parse_args()

    main(args)
