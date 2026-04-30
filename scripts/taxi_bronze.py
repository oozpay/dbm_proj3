from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder.appName("TaxiBronzeIngest").getOrCreate()

# Create namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

# Read raw parquet files
# Note: Update the path to point to your actual mounted data volume
df = spark.read.parquet("/data/taxi/*.parquet") 

# Inject the synthetic trip_id
df_with_id = df.withColumn("trip_id", monotonically_increasing_id())

# Write to Iceberg Bronze
df_with_id.write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable("lakehouse.taxi.bronze_trips")