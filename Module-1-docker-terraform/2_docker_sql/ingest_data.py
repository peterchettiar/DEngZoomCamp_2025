"""
Data ingestion script to load data into postgres
"""
import argparse
import time
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
    table_name = params.table_name

    # record start time
    start_time = time.time()

    # read the parquet file
    df = pd.read_parquet(params.url)

    # split df into a list of smaller frames
    n = 100000  # specify number of rows in each chunk
    list_df = [df[i : i + n] for i in range(0, len(df), n)]

    # initialising an engine object and passing the database connection string as an argument
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # connect to database
    connection = engine.connect()

    # write dataframe to the database table
    print(f"Writing data to table {table_name} in the database...")

    for chunk in tqdm(list_df):
        chunk.to_sql(name=table_name, con=connection, if_exists="append", index=False)

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
    parser.add_argument("--table_name", type=str, help="Table name in the database")
    parser.add_argument("--url", type=str, help="URL of the data file to ingest")

    args = parser.parse_args()

    main(args)
