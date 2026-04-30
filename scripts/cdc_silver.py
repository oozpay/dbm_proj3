import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Clear broken background environment variables
os.environ.pop("PYSPARK_SUBMIT_ARGS", None)

# 2. Detect your PySpark version
spark_version = pyspark.__version__
is_spark4 = spark_version.startswith("4")

# 3. Bundle Iceberg + AWS + Kafka packages
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
  .appName("CDC-Silver") \
  .config("spark.jars.packages", working_packages) \
  .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.lakehouse.type", "rest") \
  .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181") \
  .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
  .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
  .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
  .config("spark.sql.defaultCatalog", "lakehouse") \
  .getOrCreate()

print("Starting CDC Silver Processing...")

# 5. Create Silver table if it doesn't exist
spark.sql("""
  CREATE TABLE IF NOT EXISTS lakehouse.cdc.silver_customers (
    id INT, name STRING, email STRING, country STRING, last_updated_ms BIGINT
  ) USING iceberg
""")

# 6. Read from Bronze table
bronze_df = spark.table("lakehouse.cdc.bronze_customers")

# 7. Deduplicate records
bronze_with_key = bronze_df.withColumn(
  "entity_id", F.coalesce(F.col("after_id"), F.col("before_id"))
)

w = Window.partitionBy("entity_id").orderBy(F.col("ts_ms").desc())
deduped = bronze_with_key \
  .filter(F.col("op").isNotNull()) \
  .withColumn("rn", F.row_number().over(w)) \
  .filter("rn = 1").drop("rn")

deduped.createOrReplaceTempView("cdc_batch")

# 8. Merge into Silver table
spark.sql("""
  MERGE INTO lakehouse.cdc.silver_customers AS t
  USING cdc_batch AS s
  ON t.id = s.entity_id
  
  WHEN MATCHED AND s.op = 'd' THEN DELETE
  
  WHEN MATCHED AND s.op IN ('c','u','r') THEN UPDATE SET
    t.name = s.after_name, t.email = s.after_email,
    t.country = s.after_country, t.last_updated_ms = s.ts_ms

  WHEN NOT MATCHED AND s.op != 'd' THEN INSERT
    (id, name, email, country, last_updated_ms)
    VALUES (s.after_id, s.after_name, s.after_email, s.after_country, s.ts_ms)
""")

print("CDC Silver Processing Complete.")