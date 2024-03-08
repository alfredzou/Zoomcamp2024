import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
import time
from functools import wraps
import argparse

def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} took {end_time - start_time:.6f} seconds to run.")
        return result
    return wrapper

def parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_yellow', required = True)
    parser.add_argument('--input_green', required = True)
    parser.add_argument('--output', required = True)

    args = parser.parse_args()

    input_yellow = args.input_yellow
    input_green = args.input_green
    output = args.output

    return input_yellow, input_green, output

class MySpark:
    def __init__(self, input_yellow, input_green, output) -> None:
        self.spark = SparkSession.builder \
        .master("spark://batch-vm.australia-southeast1-b.c.sharp-harbor-411301.internal:7077") \
        .appName('test') \
        .getOrCreate()

        self.input_yellow = input_yellow
        self.input_green = input_green
        self.output = output
    
    @timer
    def read_parquet(self):
        self.green_df = self.spark.read \
        .option("header", "true") \
        .parquet(self.input_green)

        self.yellow_df = self.spark.read \
        .option("header", "true") \
        .parquet(self.input_yellow)

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
    def save_pq(self):
        self.trips_df.repartition(24).write.parquet(self.output,mode="overwrite")

if __name__ == "__main__":
    input_yellow, input_green, output = parser()
    my_spark = MySpark(input_yellow=input_yellow, input_green=input_green, output=output)

    try:
        my_spark.read_parquet()
        my_spark.rename_columns()
        my_spark.select_common_columns()
        my_spark.save_pq()
    finally:
        my_spark.spark.stop()