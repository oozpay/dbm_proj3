from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TaxiSilverClean").getOrCreate()

# Read from Bronze
df = spark.read.format("iceberg").load("lakehouse.taxi.bronze_trips")

# Clean data: Filter invalid rows and drop nulls in critical columns
df_clean = df.filter("(fare_amount > 0) AND (trip_distance > 0)") \
             .dropna(subset=["PULocationID", "tpep_pickup_datetime"])

# Write to Iceberg Silver
df_clean.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable("lakehouse.taxi.silver_trips")