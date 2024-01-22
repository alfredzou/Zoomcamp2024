import pandas as pd
from sqlalchemy import create_engine, text, inspect
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table_name = params.table_name
    csv_url = params.csv_url
    gz_name = "output.gz"

    # Download CSV data
    os.system(f"wget {csv_url} -O {gz_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    con = engine.connect()
    insp = inspect(engine)

    # Drop table
    if insp.has_table('yellow_taxi_cab'):
        con.execute(text("DROP TABLE IF EXISTS yellow_taxi_cab"))
        con.commit()
        print('dropped existing yellow_taxi_cab table')

    # Insert data in via chunks
    df_iter = pd.read_csv(gz_name, compression="gzip", iterator=True, chunksize=100000)
    for i, chunk in enumerate(df_iter, 1):
        chunk["tpep_pickup_datetime"] = pd.to_datetime(chunk["tpep_pickup_datetime"])
        chunk["tpep_dropoff_datetime"] = pd.to_datetime(chunk["tpep_dropoff_datetime"])
        chunk.to_sql(name=table_name, con=con, if_exists="append")

        rows = chunk.shape[0]
        print(f'Finished running chunk # {i} with {rows} rows')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data into Postgres")

    parser.add_argument("--user", help="host for postgres")
    parser.add_argument("--password", help="port for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--database", help="database for postgres")
    parser.add_argument("--table_name", help="table name for postgres")
    parser.add_argument("--csv_url", help="user csv url postgres")

    args = parser.parse_args()

    main(args)
