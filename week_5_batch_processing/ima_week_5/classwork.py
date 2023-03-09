from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('classwork') \
    .getOrCreate()

df_green = spark.read.parquet('data/pq/green/*/*')
df_green = df_green.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
    .withColumn("trip_type", F.lit("green")) \

df_yellow = spark.read.parquet('data/pq/yellow/*/*')
df_yellow = df_yellow.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
    .withColumn("trip_type", F.lit("yellow")) \

com_columns = [col for col in df_green.columns if col in df_yellow.columns]

union_df = df_green.select(com_columns).union(df_yellow.select(com_columns))
union_df.groupBy("trip_type").count().show()

df_zone = spark.read \
    .option("header", "true") \
    .csv('ima_week_5/taxi_zone_lookup.csv')

union_df.createOrReplaceTempView("trip_data")
df_zone.createOrReplaceTempView("zones")

df_result = spark.sql('''
SELECT 
    -- Reveneue grouping 
    zones.Zone AS revenue_zone,
    to_date(pickup_datetime) AS revenue_month, 
    trip_type, 
    
    -- Revenue calculation 
    sum(fare_amount) AS revenue_monthly_fare,
    sum(extra) AS revenue_monthly_extra,
    sum(mta_tax) AS revenue_monthly_mta_tax,
    sum(tip_amount) AS revenue_monthly_tip_amount,
    sum(tolls_amount) AS revenue_monthly_tolls_amount,

    sum(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    sum(total_amount) AS revenue_monthly_total_amount,
    sum(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    count(1) AS total_monthly_trips,
    avg(passenger_count) AS avg_montly_passenger_count,
    avg(trip_distance) AS avg_montly_trip_distance

FROM trip_data
INNER JOIN zones
ON trip_data.PULocationID = zones.LocationID
GROUP BY revenue_zone, revenue_month, trip_type
''')

df_result.coalesce(1).write.parquet("data/result/", mode="overwrite")
