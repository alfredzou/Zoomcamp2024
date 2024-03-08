import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
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

class MySpark:
    def __init__(self) -> None:
        self.spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()
    
    @timer
    def read_parquet(self):
        self.green_df = self.spark.read \
        .option("header", "true") \
        .parquet(f'parquet/green/*/*')

        self.yellow_df = self.spark.read \
        .option("header", "true") \
        .parquet(f'parquet/yellow/*/*')

        self.df_list = [self.green_df, self.yellow_df]

    @timer
    def rename_columns(self):
        self.green_df = self.green_df \
            .withColumnRenamed('lpep_pickup_datetime','pickup_datetime') \
            .withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime') \
            .withColumn('service_type',F.lit('green'))
        
        self.yellow_df = self.yellow_df \
            .withColumnRenamed('tpep_pickup_datetime','pickup_datetime') \
            .withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime') \
            .withColumn('service_type',F.lit('yellow'))
        
    @timer
    def select_common_columns(self):

        # Maintain column order
        common_cols = []
        yellow_cols = set(self.yellow_df.columns)
        for col in self.green_df.columns:
            if col in yellow_cols:
                common_cols.append(col)

        print(common_cols)

        self.green_df = self.green_df.select(common_cols) 
        self.yellow_df = self.yellow_df.select(common_cols)
        self.trips_df = self.green_df.unionAll(self.yellow_df)

    @timer
    def create_temp_table(self):
        self.trips_df.registerTempTable("trips_temp_df")

    @timer
    def run_sql(self):
        query = self.spark.sql(
            """
            SELECT service_type, COUNT(*) 
            FROM trips_temp_df
            GROUP BY 1                                      
            """)
        query.show()

if __name__ == "__main__":
    my_spark = MySpark()

    try:
        my_spark.read_parquet()
        my_spark.rename_columns()
        my_spark.select_common_columns()
        my_spark.trips_df.groupBy('service_type').count().show()
        my_spark.create_temp_table()
        my_spark.run_sql()
    finally:
        my_spark.spark.stop()