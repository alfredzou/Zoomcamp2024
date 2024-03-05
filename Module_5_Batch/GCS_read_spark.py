import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
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

class MySpark:
    def __init__(self) -> None:
        credentials_location = '/home/batch/.gcp/gcp.json'

        conf = SparkConf() \
            .setMaster('local[*]') \
            .setAppName('test') \
            .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
            .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
        
        sc = SparkContext(conf=conf)

        hadoop_conf = sc._jsc.hadoopConfiguration()

        hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
        hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
        
        self.spark = SparkSession.builder \
            .config(conf=sc.getConf()) \
            .getOrCreate()

    @timer
    def read_GCS(self):
        self.green_df = self.spark.read \
        .parquet('gs://sharp-harbor-411301-bucket/pq/green/*/*')
        
        self.green_df.show()

        print(self.green_df.count())

if __name__ == "__main__":
    
    my_spark = MySpark()
    try:
        my_spark.read_GCS()
    finally:
        my_spark.spark.stop()
