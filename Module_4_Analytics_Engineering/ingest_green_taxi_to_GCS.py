import os
import dlt
import itertools
import pandas as pd


os.environ['GOOGLE_CLOUD_PROJECT'] = 'dlt-dev-external'

def generator(year,month):
    # year = ['2020']
    # month = ['04']

    taxi_dtypes = {
            'VendorID': pd.Int64Dtype(),
            'passenger_count': pd.Int64Dtype(),
            'trip_distance': float,
            'RatecodeID':pd.Int64Dtype(),
            'store_and_fwd_flag':str,
            'PULocationID':pd.Int64Dtype(),
            'DOLocationID':pd.Int64Dtype(),
            'payment_type': pd.Int64Dtype(),
            'fare_amount': float,
            'extra':float,
            'mta_tax':float,
            'tip_amount':float,
            'tolls_amount':float,
            'improvement_surcharge':float,
            'total_amount':float,
            'congestion_surcharge':float,
            'trip_type':pd.Int64Dtype()
        }

    # native date parsing 
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month}.csv.gz"
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
    
    year = ['2019','2020']
    month = ['01','02','03','04','05','06','07','08','09','10','11','12']

    for year, month in itertools.product(year,month):
        info = pipeline.run(generator(year,month),write_disposition="append",table_name="green_taxi", loader_file_format="parquet")
        print(info)