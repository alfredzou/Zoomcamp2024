from ingest_api import paginated_getter
from ingest_stream_download import stream_download_jsonl
import dlt
import duckdb

api_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
download_url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"

# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
generators_pipeline = dlt.pipeline(pipeline_name = "taxi_data",destination='duckdb', dataset_name='generators')


# we can load any generator to a table at the pipeline destnation as follows:
info = generators_pipeline.run(paginated_getter(api_url),
                                        table_name="http_download",
                                        write_disposition="replace")

# the outcome metadata is returned by the load and we can inspect it by printing it.
print(info)

# we can load the next generator to the same or to a different table.
info = generators_pipeline.run(stream_download_jsonl(download_url),
                                        table_name="stream_download",
                                        write_disposition="replace")

print(info)

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# let's see the tables
# SET specifies the schema
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
print(conn.sql("show tables"))

# and the data

print("\n\n\n http_download table below:")

rides = conn.sql("SELECT * FROM http_download").df()
print(rides)

print("\n\n\n stream_download table below:")

passengers = conn.sql("SELECT * FROM stream_download").df()
print(passengers)

# As you can see, the same data was loaded in both cases.

