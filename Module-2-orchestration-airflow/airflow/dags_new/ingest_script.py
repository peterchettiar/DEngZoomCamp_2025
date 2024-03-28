"""
Python ingestion script to be used in DAG file
"""

import time
import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(
    user, password, host, port, db, table_name, parquet_file, execution_date
):
    """Takes arguments and uploads local parquet files into postgres"""

    print(table_name, parquet_file, execution_date)
    print("Starting data ingestion process...")

    # record start time
    start_time = time.time()

    # read the parquet file
    df = pd.read_parquet(parquet_file)

    # split df into a list of smaller frames
    n = 100000  # specify number of rows in each chunk
    list_df = [df[i : i + n] for i in range(0, len(df), n)]

    # initialising an engine object and passing the database connection string as an argument
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # connect to database
    connection = engine.connect()

    # write dataframe to the database table
    print(f"Writing data to table {table_name} in the database...")

    for chunk in list_df:
        chunk.to_sql(name=table_name, con=connection, if_exists="append", index=False)

    print("Data ingestion complete")

    # close the database connection
    connection.close()

    # Record end time
    end_time = time.time()

    # Calculate duration
    duration = end_time - start_time
    print(f"Data ingestion process completed in {duration:.2f} seconds.")
