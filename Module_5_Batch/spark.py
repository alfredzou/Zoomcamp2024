import pyspark
from pyspark.sql import SparkSession, types
import wget
import os.path
import time
from functools import wraps

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
def download_if_not_exists(url):
    if os.path.exists('fhvhv_tripdata_2021-01.csv.gz'):
        pass
    else:
        wget.download(url)

class MySpark:
    def __init__(self) -> None:
        self.spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()

    @timer
    def spark_infer_schema(self):
        self.df = self.spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv('fhvhv_tripdata_2021-01.csv.gz')

    @timer
    def spark_read_as_string(self):
        self.df = self.spark.read \
        .option("header", "true") \
        .csv('fhvhv_tripdata_2021-01.csv.gz')

    @timer
    def spark_schema_provided(self):
        schema = types.StructType([
        types.StructField('hvfhs_license_num', types.StringType(), True), 
        types.StructField('dispatching_base_num', types.StringType(), True), 
        types.StructField('pickup_datetime', types.TimestampType(), True), 
        types.StructField('dropoff_datetime', types.TimestampType(), True), 
        types.StructField('PULocationID', types.IntegerType(), True), 
        types.StructField('DOLocationID', types.IntegerType(), True), 
        types.StructField('SR_Flag', types.IntegerType(), True)
            ]
        )     

        self.df = self.spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv('fhvhv_tripdata_2021-01.csv.gz')


    @timer
    def save_parquet(self):
        self.df.write.parquet('save_parquet/',mode='overwrite')

    @timer
    def repartition_save_parquet(self):
        df = self.df.repartition(24)
        df.write.parquet('repartition_save_parquet/',mode='overwrite')
        
if __name__ == "__main__":
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz"
    download_if_not_exists(url)
    spark = MySpark()
    spark.spark_infer_schema()
    spark.spark_read_as_string()
    spark.spark_schema_provided()
    spark.save_parquet()
    spark.repartition_save_parquet()
    time.sleep(1000)
