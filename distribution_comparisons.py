from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.window import *
import numpy as np

spark = SparkSession.builder.getOrCreate()

# Load the relevant datasets
df = spark.read.csv("/user/s1919377/flights/*", header='true') \
    .withColumn("firstseen",to_timestamp("firstseen", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("lastseen",to_timestamp("lastseen", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("day",to_timestamp("day", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("longitude_1",col("longitude_1").cast("float")) \
    .withColumn("longitude_2",col("longitude_2").cast("float")) \
    .withColumn("latitude_1",col("latitude_1").cast("float")) \
    .withColumn("latitude_2",col("latitude_2").cast("float")) \
    .withColumn("altitude_1",col("altitude_1").cast("float")) \
    .withColumn("altitude_2",col("altitude_2").cast("float"))

airport_metadata = spark.read.json('file:///home/s1919377/project/airport_codes.json') \
    .withColumn("longitude",split("coordinates", ', ').getItem(0).cast("float")) \
    .withColumn("latitude",split("coordinates", ', ').getItem(1).cast("float"))

# Find the largest airports based in the first month
df_first_month = df.where((month("day") == 1) & (year("day") == 2019))

large_airports = airport_metadata.where(col("type") == "large_airport")

airports_by_destination_count = df_first_month \
    .where(col("destination").isNotNull()) \
    .groupby("destination") \
    .count() \
    .join(large_airports, [large_airports.ident == col("destination")], "leftsemi")

top_100_airports = airports_by_destination_count \
    .sort(col("count").desc()) \
    .limit(100)

# Get the distribution of the largest airports at three dates
df_first_month = df \
    .where((month("day") == 1) & (year("day") == 2019)) \
    .join(top_100_airports, "destination", "leftsemi")
df_last_month = df \
    .where((month("day") == 12) & (year("day") == 2021)) \
    .join(top_100_airports, "destination", "leftsemi")
df_mid_month = df \
    .where((month("day") == 4) & (year("day") == 2020)) \
    .join(top_100_airports, "destination", "leftsemi")

df_first_month_counts = df_first_month \
    .groupby("destination") \
    .count() \
    .withColumn("ix", dense_rank().over(Window.orderBy(col("count").desc()))) \
    .persist()
df_last_month_counts = df_last_month \
    .groupby("destination") \
    .count() \
    .withColumnRenamed("count", "count_temp") \
    .join(df_first_month_counts, "destination", "left") \
    .drop("count") \
    .withColumnRenamed("count_temp", "count") \
    .sort(col("ix").asc()) \
    .persist()
df_mid_month_counts = df_mid_month \
    .groupby("destination") \
    .count() \
    .withColumnRenamed("count", "count_temp") \
    .join(df_first_month_counts, "destination", "left") \
    .drop("count") \
    .withColumnRenamed("count_temp", "count") \
    .sort(col("ix").asc()) \
    .persist()

# Run linear regression on all three time windows
vectorAssembler = VectorAssembler(inputCols = ['ix'], outputCol = 'features')

vfirst_df = vectorAssembler.transform(df_first_month_counts)
vfirst_df = vfirst_df.select(['features', 'count'])

lr_first = LinearRegression(featuresCol='features', labelCol='count', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model_first = lr_first.fit(vfirst_df)
print("Coefficients: " + str(lr_model_first.coefficients))
print("Intercept: " + str(lr_model_first.intercept))
trainingSummary_first = lr_model_first.summary
print("RMSE: %f" % trainingSummary_first.rootMeanSquaredError)
print("r2: %f" % trainingSummary_first.r2)

vlast_df = vectorAssembler.transform(df_last_month_counts)
vlast_df = vlast_df.select(['features', 'count'])

lr_last = LinearRegression(featuresCol='features', labelCol='count', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model_last = lr_last.fit(vlast_df)
print("Coefficients: " + str(lr_model_last.coefficients))
print("Intercept: " + str(lr_model_last.intercept))
trainingSummary_last = lr_model_last.summary
print("RMSE: %f" % trainingSummary_last.rootMeanSquaredError)
print("r2: %f" % trainingSummary_last.r2)

vmid_df = vectorAssembler.transform(df_mid_month_counts)
vmid_df = vmid_df.select(['features', 'count'])

lr_mid = LinearRegression(featuresCol='features', labelCol='count', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_mid_last = lr_mid.fit(vmid_df)
print("Coefficients: " + str(lr_mid_last.coefficients))
print("Intercept: " + str(lr_mid_last.intercept))
trainingSummary_mid = lr_mid_last.summary
print("RMSE: %f" % trainingSummary_mid.rootMeanSquaredError)
print("r2: %f" % trainingSummary_mid.r2)
