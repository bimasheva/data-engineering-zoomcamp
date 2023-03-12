import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder \
    .appName("dataproc-implementation") \
    .getOrCreate()

df_green = spark.read.parquet(input_green)
df_green = df_green.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
    .withColumn("trip_type", F.lit("green"))

df_yellow = spark.read.parquet(input_yellow)
df_yellow = df_yellow.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
    .withColumn("trip_type", F.lit("yellow"))

com_columns = [col for col in df_green.columns if col in df_yellow.columns]

union_df = df_green.select(com_columns).union(df_yellow.select(com_columns))

union_df.createOrReplaceTempView("trip_data")

df_result = spark.sql('''
                    SELECT 
                        -- Reveneue grouping 
                        PULocationID AS revenue_zone,
                        date_trunc('month', pickup_datetime) AS revenue_month, 
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
                    GROUP BY revenue_zone, revenue_month, trip_type
                    '''
                      )

# df_result.coalesce(1).write.parquet(output, mode="overwrite")
df_result.write.format('bigquery') \
  .option('table', 'wordcount_dataset.wordcount_output') \
  .save()