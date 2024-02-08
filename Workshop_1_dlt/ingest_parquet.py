import os
import dlt
import parquet
import json
import glob
from ingest_stream_download import stream_download_jsonl

# Set the bucket_url. We can also use a local folder
os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = 'C:\\Users\\alfre\\Dropbox\\Study Break\\Data Engineering Zoomcamp\\Workshop_1_dlt\\.dlt\\my_folder'

url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"
# Define your pipeline
pipeline = dlt.pipeline(
    pipeline_name='my_pipeline',
    destination='filesystem',
    dataset_name='mydata'
)



# Run the pipeline with the generator we created earlier.
load_info = pipeline.run(stream_download_jsonl(url), table_name="users", loader_file_format="parquet")

print(load_info)

# Get a list of all Parquet files in the specified folder
parquet_files = glob.glob('C:\\Users\\alfre\\Dropbox\\Study Break\\Data Engineering Zoomcamp\\Workshop_1_dlt\\.dlt\\my_folder\\mydata\\users\\*.parquet')

# show parquet files
print("Loaded files: ")
for file in parquet_files:
  print(file)