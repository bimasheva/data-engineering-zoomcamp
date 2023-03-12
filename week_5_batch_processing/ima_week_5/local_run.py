from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# spark = SparkSession.builder \
#     .master("spark://MacBook-Pro-MacBook.local:7077") \
#     .appName('classwork') \
#     .getOrCreate()

config = SparkConf() \
    .setAll([('spark.executor.memory', '2g'),
             ('spark.executor.cores', '3'),
             ('spark.cores.max', '3'),
             ('spark.driver.memory', '2g'),
             ('spark.executor.memoryOverhead', '4096'),
             ('spark.driver.memoryOverhead', '4096')])

spark = SparkSession.builder \
    .master("spark://MacBook-Pro-MacBook.local:7077") \
    .appName('classwork') \
    .config(conf=config).getOrCreate()

df_green = spark.read.parquet('data/pq/green/2020/05')
df_green = df_green.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
    .withColumn("trip_type", F.lit("green"))

df_green.show()
