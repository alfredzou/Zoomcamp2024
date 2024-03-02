import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import time
import wget, os
from functools import wraps
import itertools
import re

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
    file_name = re.search("([^/]+)$", url)[0]
    print(file_name)
    if os.path.exists(file_name):
        print(f"{file_name} already exists. Skipping")
    else:
        wget.download(url)

class MySpark:
    def __init__(self) -> None:
        self.spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()
    
    @timer
    def read_csv_fhv(self):
        self.fhv_df = self.spark.read \
        .option("header", "true") \
        .csv('fhv_tripdata_2019-10.csv.gz')
    
    @timer
    def read_csv_taxi_zone(self):
        self.lookup_df = self.spark.read \
        .option("header", "true") \
        .csv('taxi_zone_lookup.csv')

    @timer
    def repartition_save(self):
        self.fhv_df \
        .repartition(6) \
        .write.parquet('pq/', mode = "overwrite")

    @timer
    def create_temp_table(self):
        self.fhv_df.registerTempTable("fhv")
        self.lookup_df.registerTempTable("lookup")

    @timer
    def run_sql(self):
        print("Q3 # trips started on 15th Oct")
        query = self.spark.sql(
            """
            SELECT COUNT(*) 
            FROM fhv
            WHERE DATE(pickup_datetime) = "2019-10-15"
            """)
        query.show()

        print("Q4 Longest Trip")
        query = self.spark.sql(
            """
            SELECT DATEDIFF(hour, pickup_datetime, dropOff_datetime) AS duration
            FROM fhv 
            ORDER BY 1 DESC
            """)
        query.show()

        print("Q6 Least frequent pickup zone")
        query = self.spark.sql(
            """
            SELECT lookup.Zone, COUNT(*) 
            FROM fhv
            LEFT JOIN lookup
            ON fhv.PUlocationID = lookup.LocationID
            GROUP BY 1
            ORDER BY COUNT(*) ASC
            """)
        query.show()

if __name__ == "__main__":
    fhv_url ="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz"
    zone_lookup_url ="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    download_if_not_exists(fhv_url)
    download_if_not_exists(zone_lookup_url)

    my_spark = MySpark()

    try:
        my_spark.read_csv_fhv()
        my_spark.read_csv_taxi_zone()
        my_spark.repartition_save()
        my_spark.fhv_df.show()
        my_spark.lookup_df.show()
        my_spark.create_temp_table()
        my_spark.run_sql()
    finally:
        my_spark.spark.stop()