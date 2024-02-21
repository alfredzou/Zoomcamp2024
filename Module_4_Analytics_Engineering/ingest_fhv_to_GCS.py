import os
import dlt
import itertools
import pandas as pd


os.environ['GOOGLE_CLOUD_PROJECT'] = 'dlt-dev-external'

def generator(year,month):

    taxi_dtypes = {
            'dispatching_base_num': str,
            'PUlocationID':float,
            'DOlocationID':float,
            'SR_Flag': pd.Int64Dtype(),
            'Affiliated_base_number': str
        }

    # native date parsing 
    parse_dates = ['pickup_datetime', 'dropOff_datetime']
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month}.csv.gz"
    print(url)
    for chunk in pd.read_csv(url,chunksize=1000,compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates):
        yield chunk

# Define your pipeline


if __name__ == "__main__":
    pipeline = dlt.pipeline(
    pipeline_name='my_pipeline_mod4',
    destination='filesystem',
    dataset_name='ny_taxi_mod4'
    )
    
    year = ['2019']
    month = ['01','02','03','04','05','06','07','08','09','10','11','12']

    for year, month in itertools.product(year,month):
        info = pipeline.run(generator(year,month),write_disposition="append",table_name="fhv", loader_file_format="parquet")
        print(info)