import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os
import re
import time
import argparse


def create_connection(user, password, host, port, database):
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    con = engine.connect()
    insp = inspect(engine)
    return con, insp


def ingest_data(url, con, insp, table):
    if insp.has_table(table):
        con.execute(text(f"DROP TABLE IF EXISTS {table}"))
        con.commit()
        print(f"dropped table {table}")

    os.system(f"wget {url}")
    pattern = r"(?<=/)([^/]+)$"
    file_name = re.search(pattern, url)[0]
    df_iter = enumerate(pd.read_csv(file_name, chunksize=100000), 1)

    for i, chunk in df_iter:
        start = time.time()
        if table == "green_taxi_trip":
            chunk["lpep_pickup_datetime"] = pd.to_datetime(
                chunk["lpep_pickup_datetime"]
            )
            chunk["lpep_dropoff_datetime"] = pd.to_datetime(
                chunk["lpep_dropoff_datetime"]
            )
        chunk.to_sql(name=table, index=False, con=con, if_exists="append")

        row = chunk.shape[0]
        end = time.time()
        print(
            f"loaded chunk {i} with {row} rows into {table}. Took {end-start:.3f} seconds"
        )


def ingest_green_taxi_data(url, con, insp):
    table = "green_taxi_trip"
    ingest_data(url, con, insp, table)


def ingest_taxi_zone_lookup_data(url, con, insp):
    table = "taxi_zone_lookup"
    ingest_data(url, con, insp, table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", help="the postgres database user")
    parser.add_argument("--password", help="the postgres database password")
    parser.add_argument("--host", help="the postgres database host")
    parser.add_argument("--port", help="the postgres database port")
    parser.add_argument("--database", help="the database to ingest data into")
    parser.add_argument(
        "--ingest_type",
        help="the data type to ingest (green_taxi_trip or taxi_zone_lookup)",
    )
    parser.add_argument("--url", help="the data url")

    args = parser.parse_args()
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    database = args.database
    ingest_type = args.ingest_type
    url = args.url

    try:
        con, insp = create_connection(user, password, host, port, database)
        if ingest_type == "green_taxi_trip":
            ingest_green_taxi_data(url, con, insp)
        elif ingest_type == "taxi_zone_lookup":
            ingest_taxi_zone_lookup_data(url, con, insp)
        else:
            print("Pass either 'green_taxi_trip' or 'taxi_zone_lookup' for ingest_type")
    finally:
        con.close()

    """
    python -m ingest \
    --user root \
    --password root \
    --host localhost \
    --port 5432 \
    --database taxi_ny \
    --ingest_type green_taxi_trip \
    --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz
    """

    """
    python -m ingest \
    --user root \
    --password root \
    --host localhost \
    --port 5432 \
    --database taxi_ny \
    --ingest_type taxi_zone_lookup \
    --url https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
    """
