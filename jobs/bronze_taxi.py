from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("bronze_taxi")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

table = "lakehouse.taxi.bronze_taxi"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table} (
  topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP,
  raw_json STRING
) USING iceberg
""")

raw = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "taxi-trips") \
    .option("startingOffsets", "earliest") \
    .load()

df = raw.filter(F.col("value").isNotNull()).select(
    F.col("topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.col("value").cast("string").alias("raw_json")
)

existing = spark.table(table).select("topic", "kafka_partition", "kafka_offset")

new_rows = df.join(
    existing,
    on=["topic", "kafka_partition", "kafka_offset"],
    how="left_anti"
)

count = new_rows.count()

if count > 0:
    new_rows.writeTo(table).append()

print(f"bronze_taxi: appended {count} new rows")

spark.stop()