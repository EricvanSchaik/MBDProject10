from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from nuts_finder import NutsFinder
import sys
import builtins as py_builtin
from scipy import signal
import numpy as np
import datetime
import pycountry

# Initial Setup
nf = NutsFinder()
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

europe_covid_cases = spark.read.option("multiline","true").json(f'/user/s1919377/14_day_daily_covid.json') \
    .withColumn("date", to_date("date", "yyyyMMdd")) \
    .withColumn("rate_14_day_per_100k", col("rate_14_day_per_100k").cast("float")) \
    .fillna(0, "rate_14_day_per_100k")

covid_international = spark.read.csv("/user/s1919377/covid_international.csv", header='true')
covid_international = covid_international \
    .select("iso_code", "continent", "location", "date", "total_cases", "total_deaths", "total_cases_per_million", "total_deaths_per_million", "new_cases_per_million") \
    .withColumn("date", to_date("date", "yyyy-MM-dd")) \
    .withColumn("total_cases_per_million", col("total_cases_per_million").cast("float")) \
    .withColumn("new_cases_per_million", col("new_cases_per_million").cast("float")) \
    .withColumn("total_cases", col("total_cases").cast("float")) \
    .withColumn("total_deaths", col("total_deaths").cast("float")) \
    .withColumn("total_deaths_per_million", col("total_deaths_per_million").cast("float")) \
    .fillna(0, ["total_cases", "total_deaths", "total_cases_per_million", "total_deaths_per_million", "new_cases_per_million"])

# Filter and improve the data

## Only consider 'large' airports
large_airports = airport_metadata.where(col("type") == "large_airport")

## Find the relevant data in europe
europe_bounds = [
    [-24.0, 34.41],
    [49.98, 71.28]
]

#### This is used to find a european NUTS code based on longitude and latitude
def find_nuts(longitude, latitude):
    nuts_info = nf.find(lat=latitude, lon=longitude)
    if len(nuts_info) > 0:
        return [nuts_info[0]["NUTS_ID"],nuts_info[1]["NUTS_ID"],nuts_info[2]["NUTS_ID"],nuts_info[3]["NUTS_ID"]]
    else:
        return None
nuts_udf = udf(find_nuts, ArrayType(StringType()))

### Large airports in europe
large_european_airports = large_airports \
    .where((col('continent') == 'EU')) \
    .withColumn("nuts", nuts_udf(col("longitude"), col("latitude"))) \
    .where(col("nuts").isNotNull())

### The date range to examine
total_date_range = ("2020-03-01",  "2021-11-30")

airports_by_destination_count = df \
    .where(col("destination").isNotNull()) \
    .groupby("destination") \
    .count() \
    .join(large_airports, [large_airports.ident == col("destination")], "leftsemi")

top_100_airports = airports_by_destination_count \
    .sort(col("count").desc()) \
    .limit(100)

bottom_100_airports = airports_by_destination_count \
    .sort(col("count").asc()) \
    .limit(100)
    
### Filter flights based on the top and bottom airports
df_top_100 = df.join(top_100_airports, "destination", "leftsemi")
df_bottom_100 = df.join(bottom_100_airports, "destination", "leftsemi")

### Filter flights based on airport hubs
df_eham = df.where(col("destination") == "EHAM")
df_klax = df.where(col("destination") == "KLAX")
df_omdb = df.where(col("destination") == "OMDB")

### Filter flights in europe
df_europe = df.join(large_european_airports, [(df.origin == large_european_airports.ident) | (df.destination == large_european_airports.ident)], 'leftsemi')

# Combine flight counts with covid cases

## This udf is used to match the ISO Alpha-3 in the cases dataset with the ISO Alpha-2 in the airports dataset
def convert_alpha2_to_alpha3(iso_2):
    country = pycountry.countries.get(alpha_2=iso_2)
    if country is None:
        return None
    return country.alpha_3
convert_iso_udf = udf(convert_alpha2_to_alpha3, StringType())

## All input dfs to calculate flight counts and cases for
input_dfs = {
    "top_100": df_top_100,
    "bottom_100": df_bottom_100,
    "eham": df_eham,
    "klax": df_klax,
    "omdb": df_omdb,
    "europe": df_europe
}

counts_cases = {
    key:value \
        .join(large_airports, [large_airports.ident == col("destination")], "left") \
        .where(col("ident").isNotNull()) \
        .groupby("day", "iso_country", "ident", "name") \
        .count() \
        .withColumn("iso_country", convert_iso_udf(col("iso_country"))) \
        .where(col("iso_country").isNotNull()) \
        .join(covid_international, [(to_date(col("day")) == covid_international.date) & (col("iso_country") == covid_international.iso_code)], "inner") \
        .drop("date", "nuts", "iso_code") \
        .sort(col("day").asc()) \
        .where(col("day").between(*total_date_range)) \
        .withColumn("period",
            when(col("day").between("2020-03-01", "2020-06-01"), "Q1")
            .when(col("day").between("2020-06-01", "2020-09-01"), "Q2")
            .when(col("day").between("2020-09-01", "2020-12-01"), "Q3")
            .when(col("day").between("2020-12-01", "2021-03-01"), "Q4")
            .when(col("day").between("2021-03-01", "2021-06-01"), "Q5")
            .when(col("day").between("2021-06-01", "2021-09-01"), "Q6")
            .when(col("day").between("2021-09-01", "2021-12-01"), "Q7")
        ) \
    for (key,value) in input_dfs.items()
}

# Get aggregate data of the datasets

## This is used to get the flight to cases lag by cross correlating across different timespans
## The highest correlation indicates the correct timeshift
corr_schema = StructType([
    StructField("lag", FloatType(), False),
    StructField("correlation", FloatType(), False)
])

timeshift_bound = 100
@pandas_udf(ArrayType(FloatType()), functionType=PandasUDFType.GROUPED_AGG)
def lag_finder_udf(y1, y2):
    n = len(y1)
    n_bounded = py_builtin.min(n, timeshift_bound)

    corr = signal.correlate(y2, y1, mode='same') / np.sqrt(signal.correlate(y1, y1, mode='same')[int(n/2)] * signal.correlate(y2, y2, mode='same')[int(n/2)])

    delay_arr = np.linspace(-0.5*n, 0.5*n, n)
    argmax = int(n/2 - n_bounded) + np.argmax(corr[int(0.5*n)-n_bounded:int(0.5*n)+n_bounded])
    delay = delay_arr[argmax]
    return [delay, corr[argmax]]


## Calculate the aggregates for all input dfs
for name, counts_cases_df in counts_cases.items():
    counts_cases_df = counts_cases_df.persist()
    ## Get and store timeshift and correlation by airport
    counts_cases_df \
        .groupBy("ident", "name") \
        .agg(lag_finder_udf("count", "new_cases_per_million").alias("corrs")) \
        .withColumn("lag", col("corrs").getItem(0)) \
        .withColumn("correlation", col("corrs").getItem(1)) \
        .drop("corrs") \
        .write.csv(name + "_airport_aggregates.csv", header=True, mode="overwrite")

    ## Get and store timeshift and correlation by airport by period
    counts_cases_df \
        .groupBy("ident", "period", "name") \
        .agg(lag_finder_udf("count", "new_cases_per_million").alias("corrs")) \
        .withColumn("lag", col("corrs").getItem(0)) \
        .withColumn("correlation", col("corrs").getItem(1)) \
        .drop("corrs") \
        .write.csv(name + "_airport_aggregates_by_period.csv", header=True, mode="overwrite")

    ## Get and store the total aggregates by day
    counts_cases_df \
        .groupBy("day") \
        .agg(
            mean("count").alias("mean_count"),
            mean("new_cases_per_million").alias("mean_cases")
        ) \
        .sort(col("day").asc()) \
        .write.csv(name + "_date_aggregates.csv", header=True, mode="overwrite")

    ## Get and store the total aggregates by period
    counts_cases_df \
        .groupBy("period", "ident") \
        .agg(
            corr("count", "new_cases_per_million").alias("correlation"),
            mean("count").alias("mean_count"),
            mean("new_cases_per_million").alias("mean_cases"),
            min("day").alias("range_start"),
            max("day").alias("range_end")
        ) \
        .groupBy("period") \
        .agg(
            mean("mean_count").alias("mean_count"),
            mean("mean_cases").alias("mean_cases"),
            mean("correlation").alias("mean_correlation"),
            min("range_start").alias("range_start"),
            max("range_end").alias("range_end")
        ) \
        .sort(col("period").asc()) \
        .write.csv(name + "_date_aggregates_by_period.csv", header=True, mode="overwrite")
