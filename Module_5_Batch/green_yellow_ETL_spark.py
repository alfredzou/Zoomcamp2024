import pyspark
from pyspark.sql import SparkSession, types
import wget
import os.path
import time
from functools import wraps
import itertools

def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} took {end_time - start_time:.6f} seconds to run.")
        return result
    return wrapper

@timer
def download_if_not_exists(taxi_type):
    year = ['2020','2021']
    month = [f"{i:02}" for i in range(1,13)]
    for year, month in itertools.product(year, month):
        if taxi_type in ('green','yellow'):
            url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{month}.csv.gz"
        else:
            raise Exception('type should be "yellow" or "green"')
        
        output_folder = f"{taxi_type}/{year}/{month}"
        os.makedirs(output_folder,exist_ok=True)

        output_path = f"{output_folder}/{taxi_type}_tripdata_{year}-{month}.csv.gz"

        if os.path.exists(output_path):
            print(f'{output_path} already exists. Skipping')
        else:
            try:
                print(f'downloading {output_path}')
                wget.download(url,out=output_path)
            except Exception as e:
                print(f"Error downloading file: {e}")

class MySpark:
    def __init__(self,type) -> None:
        self.spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()

        self.type = type

    @timer
    def spark_schema_provided(self, year, month):
        if self.type == 'green':
            schema = \
            types.StructType([
                types.StructField('VendorID', types.IntegerType(), True), 
                types.StructField('lpep_pickup_datetime', types.TimestampType(), True), 
                types.StructField('lpep_dropoff_datetime', types.TimestampType(), True), 
                types.StructField('store_and_fwd_flag', types.StringType(), True), 
                types.StructField('RatecodeID', types.IntegerType(), True), 
                types.StructField('PULocationID', types.IntegerType(), True), 
                types.StructField('DOLocationID', types.IntegerType(), True), 
                types.StructField('passenger_count', types.IntegerType(), True), 
                types.StructField('trip_distance', types.DoubleType(), True), 
                types.StructField('fare_amount', types.DoubleType(), True), 
                types.StructField('extra', types.DoubleType(), True), 
                types.StructField('mta_tax', types.DoubleType(), True), 
                types.StructField('tip_amount', types.DoubleType(), True), 
                types.StructField('tolls_amount', types.DoubleType(), True), 
                types.StructField('ehail_fee', types.StringType(), True), 
                types.StructField('improvement_surcharge', types.DoubleType(), True), 
                types.StructField('total_amount', types.DoubleType(), True), 
                types.StructField('payment_type', types.IntegerType(), True), 
                types.StructField('trip_type', types.IntegerType(), True), 
                types.StructField('congestion_surcharge', types.DoubleType(), True)
             ])
        elif self.type == 'yellow':
            schema = \
            types.StructType([
                types.StructField('VendorID', types.IntegerType(), True), 
                types.StructField('tpep_pickup_datetime', types.TimestampType(), True), 
                types.StructField('tpep_dropoff_datetime', types.TimestampType(), True), 
                types.StructField('passenger_count', types.IntegerType(), True), 
                types.StructField('trip_distance', types.DoubleType(), True), 
                types.StructField('RatecodeID', types.IntegerType(), True), 
                types.StructField('store_and_fwd_flag', types.StringType(), True), 
                types.StructField('PULocationID', types.IntegerType(), True), 
                types.StructField('DOLocationID', types.IntegerType(), True), 
                types.StructField('payment_type', types.IntegerType(), True), 
                types.StructField('fare_amount', types.DoubleType(), True), 
                types.StructField('extra', types.DoubleType(), True), 
                types.StructField('mta_tax', types.DoubleType(), True), 
                types.StructField('tip_amount', types.DoubleType(), True), 
                types.StructField('tolls_amount', types.DoubleType(), True), 
                types.StructField('improvement_surcharge', types.DoubleType(), True), 
                types.StructField('total_amount', types.DoubleType(), True), 
                types.StructField('congestion_surcharge', types.DoubleType(), True)
            ])
        
        return self.spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(f'{self.type}/{year}/{month}')
        
    @timer
    def repartition_save_parquet(self, df, year, month):
        df.repartition(4).write.parquet(f'parquet/{self.type}/{year}/{month}',mode='overwrite')
        
if __name__ == "__main__":
    for taxi_type in ['green','yellow']:
        download_if_not_exists(taxi_type)

    green_spark = MySpark('green')
    
    year = ['2020','2021']
    month = [f"{i:02}" for i in range(1,13)]
    for year, month in itertools.product(year, month):
        df = green_spark.spark_schema_provided(year, month)
        green_spark.repartition_save_parquet(df, year, month)


    year = ['2020','2021']
    month = [f"{i:02}" for i in range(1,13)]
    yellow_spark = MySpark('yellow')

    for year, month in itertools.product(year, month):
        df = yellow_spark.spark_schema_provided(year, month)
        yellow_spark.repartition_save_parquet(df, year, month)