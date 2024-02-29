import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
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

    
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'
    
class MySpark:
    def __init__(self) -> None:
        self.spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()

    @timer
    def repartition_load_parquet(self):
        self.df = self.spark.read.parquet('repartition_save_parquet/',mode='overwrite')
    
    @timer
    def datetime_to_date(self):
        self.df \
            .withColumn('pickup_date',F.to_date(self.df.pickup_datetime)) \
            .withColumn('dropoff_date',F.to_date(self.df.dropoff_datetime)) \
            .select('pickup_date','dropoff_date','PULocationID','DOLocationID') \
            .show()

    def print_schema(self):
        self.df.printSchema()

    @timer
    def datetime_to_date(self):
        self.df \
            .withColumn('pickup_date',F.to_date(self.df.pickup_datetime)) \
            .withColumn('dropoff_date',F.to_date(self.df.dropoff_datetime)) \
            .select('pickup_date','dropoff_date','PULocationID','DOLocationID') \
            .show()
        
    @timer
    def udf(self):
        crazy_stuff_udf = F.udf(crazy_stuff,returnType=types.StringType())

        self.df \
            .withColumn('base_id',crazy_stuff_udf(self.df.dispatching_base_num)) \
            .select('base_id') \
            .show()

if __name__ == "__main__":
    my_spark = MySpark()
    try:
        my_spark.repartition_load_parquet()
        my_spark.print_schema()
        my_spark.datetime_to_date()
        my_spark.udf()
    finally:
        my_spark.spark.stop()
