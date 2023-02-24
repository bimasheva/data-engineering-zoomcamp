from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True),
])

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .schema(schema) \
    .option("header", "true") \
    .csv('fhvhv_tripdata_2021-06.csv.gz')

### Question 1 and 2
# df = df.repartition(12)
# df.write.parquet('fhvhv/2021/06/')
#
# print("Finished")
#

### Question 3
# row_cnt = df.filter(F.to_date(df.pickup_datetime) == "2021-06-15").count()
# print(row_cnt)
#

### Question 4
# df = df.withColumn("trip_duration", (df.dropoff_datetime.cast("long") - df.pickup_datetime.cast("long"))/3600) \
#     .withColumn("pickup_date", F.to_date(df.pickup_datetime)) \
#     .select("pickup_date", "trip_duration") \
#     .groupBy("pickup_date").max("trip_duration") \
#     .sort(F.desc("max(trip_duration)")) \
#     .show()

# Question 6
df_zone = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')

df_agg = df.groupBy("PULocationID") \
    .agg(F.count("dispatching_base_num").alias("trip_cnt")) \
    .select("PULocationID", "trip_cnt") \
    .sort(F.desc("trip_cnt")) \
    .limit(1)

df_join = df_agg.join(df_zone, df_agg.PULocationID == df_zone.LocationID, "left")
df_join.show(truncate=False)


