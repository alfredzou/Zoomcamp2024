import os
import dlt
import itertools
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import requests
from pathlib import Path
import shutil


os.environ['GOOGLE_CLOUD_PROJECT'] = 'dlt-dev-external'

def download_pq(year,month):
    Path("parquet_files").mkdir(parents=True, exist_ok=True)

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet"
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with open(f"parquet_files/green_tripdata_{year}-{month}.parquet", "wb") as f:
            for chunk in response.iter_content(chunk_size=10000):  
                if chunk:
                    f.write(chunk)
    else:
        print("Failed to download the Parquet file. Status code:", response.status_code)

def delete_pq_dir():
    directory_path = r"C:\Users\alfre\Dropbox\Study Break\Data Engineering Zoomcamp\Module_3_Bigquery\homework\parquet_files"

    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
        print(f"{directory_path} has been deleted successfully.")
    else:
        print(f"The directory {directory_path} does not exist.")

def generator(year,month):
    pq_path = f"parquet_files/green_tripdata_{year}-{month}.parquet"

    parquet_file = pq.ParquetFile(pq_path)
    for batch in parquet_file.iter_batches(batch_size=1000):
        yield batch

if __name__ == "__main__":
    pipeline = dlt.pipeline(
    pipeline_name='my_pipeline_mod3_hw',
    destination='filesystem',
    dataset_name='ny_taxi'
    )
    
    delete_pq_dir()   

    year_list = ['2022']
    month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']
    
    for year, month in itertools.product(year_list,month_list):
        print(year,month)
        download_pq(year,month)

    for year, month in itertools.product(year_list,month_list):
        print(year,month)
        info = pipeline.run(generator(year,month),write_disposition="append",table_name="green_taxi", loader_file_format="parquet")
        print(info)