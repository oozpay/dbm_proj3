from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("bronze-cdc")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")

tables = ["customers", "drivers"]

for table in tables:
    topic = f"dbserver1.public.{table}"
    target = f"lakehouse.cdc.bronze_{table}"

    raw = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    df = raw.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("raw_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.when(F.col("value").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_tombstone"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.op").alias("op"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.ts_ms").cast("long").alias("ts_ms"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.before").alias("before_json"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.after").alias("after_json"),
        F.current_timestamp().alias("ingested_at")
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target} (
            topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            raw_key STRING,
            raw_value STRING,
            is_tombstone BOOLEAN,
            op STRING,
            ts_ms BIGINT,
            before_json STRING,
            after_json STRING,
            ingested_at TIMESTAMP
        )
        USING iceberg
    """)

    existing = spark.table(target).select("topic", "kafka_partition", "kafka_offset")

    new_rows = (
        df.alias("n")
        .join(
            existing.alias("e"),
            on=[
                F.col("n.topic") == F.col("e.topic"),
                F.col("n.kafka_partition") == F.col("e.kafka_partition"),
                F.col("n.kafka_offset") == F.col("e.kafka_offset")
            ],
            how="left_anti"
        )
    )

    count = new_rows.count()

    if count > 0:
        new_rows.writeTo(target).append()

    print(f"{table}: appended {count} new CDC rows to {target}")

spark.stop()
