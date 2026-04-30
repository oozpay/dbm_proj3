import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Clear broken background environment variables
os.environ.pop("PYSPARK_SUBMIT_ARGS", None)

# 2. Detect your PySpark version to ensure perfect compatibility
spark_version = pyspark.__version__
is_spark4 = spark_version.startswith("4")

# 3. Bundle Iceberg + AWS + Kafka packages together
if is_spark4:
    working_packages = (
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,"
        "org.apache.iceberg:iceberg-aws-bundle:1.10.0,"
        f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"
    )
else:
    working_packages = (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"
    )

# 4. Start the session
spark = SparkSession.builder \
  .appName("CDC-Bronze") \
  .config("spark.jars.packages", working_packages) \
  .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.lakehouse.type", "rest") \
  .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181") \
  .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
  .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
  .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
  .config("spark.sql.defaultCatalog", "lakehouse") \
  .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")

print("Starting CDC Bronze Ingestion...")

# 5. Read from Kafka
raw = spark.read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "dbserver1.public.customers") \
  .option("startingOffsets", "earliest") \
  .load()

# 6. Parse and transform
raw_filtered = raw.filter(F.col("value").isNotNull())

bronze_df = raw_filtered.select(
  F.col("topic"),
  F.col("partition").alias("kafka_partition"),
  F.col("offset").alias("kafka_offset"),
  F.col("timestamp").alias("kafka_timestamp"),
  F.get_json_object(F.col("value").cast("string"), "$.payload.op").alias("op"),
  F.get_json_object(F.col("value").cast("string"), "$.payload.ts_ms").cast("long").alias("ts_ms"),
  F.get_json_object(F.col("value").cast("string"), "$.payload.after.id").cast("int").alias("after_id"),
  F.get_json_object(F.col("value").cast("string"), "$.payload.after.name").alias("after_name"),
  F.get_json_object(F.col("value").cast("string"), "$.payload.after.email").alias("after_email"),
  F.get_json_object(F.col("value").cast("string"), "$.payload.after.country").alias("after_country"),
  F.get_json_object(F.col("value").cast("string"), "$.payload.before.id").cast("int").alias("before_id"),
)

# 7. Write to Bronze Iceberg table
bronze_df.writeTo("lakehouse.cdc.bronze_customers").createOrReplace()
print("CDC Bronze Ingestion Complete.")