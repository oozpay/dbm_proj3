from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("silver_taxi")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

bronze = "lakehouse.taxi.bronze_taxi"
silver = "lakehouse.taxi.silver_taxi"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver} (
  trip_id BIGINT,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  passenger_count INT,
  trip_distance DOUBLE,
  pickup_location_id INT,
  dropoff_location_id INT,
  fare_amount DOUBLE,
  total_amount DOUBLE,
  pickup_zone STRING,
  pickup_borough STRING,
  dropoff_zone STRING,
  dropoff_borough STRING,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP
) USING iceberg
""")

bronze_df = spark.table(bronze)

parsed = bronze_df.select(
    F.col("kafka_offset").alias("trip_id"),
    F.to_timestamp(F.get_json_object("raw_json", "$.tpep_pickup_datetime")).alias("pickup_datetime"),
    F.to_timestamp(F.get_json_object("raw_json", "$.tpep_dropoff_datetime")).alias("dropoff_datetime"),
    F.get_json_object("raw_json", "$.passenger_count").cast("double").cast("int").alias("passenger_count"),
    F.get_json_object("raw_json", "$.trip_distance").cast("double").alias("trip_distance"),
    F.get_json_object("raw_json", "$.PULocationID").cast("double").cast("int").alias("pickup_location_id"),
    F.get_json_object("raw_json", "$.DOLocationID").cast("double").cast("int").alias("dropoff_location_id"),
    F.get_json_object("raw_json", "$.fare_amount").cast("double").alias("fare_amount"),
    F.get_json_object("raw_json", "$.total_amount").cast("double").alias("total_amount"),
    F.col("kafka_offset"),
    F.col("kafka_timestamp")
)

clean = parsed.filter(
    (F.col("pickup_datetime").isNotNull()) &
    (F.col("dropoff_datetime").isNotNull()) &
    (F.col("dropoff_datetime") > F.col("pickup_datetime")) &
    (F.col("trip_distance") > 0) &
    (F.col("fare_amount") >= 0) &
    (F.col("total_amount") >= 0) &
    (F.col("pickup_location_id").isNotNull()) &
    (F.col("dropoff_location_id").isNotNull())
)

zones = spark.read.parquet("/home/jovyan/project/data/taxi_zone_lookup.parquet")

pickup_zones = zones.select(
    F.col("LocationID").alias("pickup_location_id"),
    F.col("Zone").alias("pickup_zone"),
    F.col("Borough").alias("pickup_borough")
)

dropoff_zones = zones.select(
    F.col("LocationID").alias("dropoff_location_id"),
    F.col("Zone").alias("dropoff_zone"),
    F.col("Borough").alias("dropoff_borough")
)

enriched = clean \
    .join(pickup_zones, on="pickup_location_id", how="left") \
    .join(dropoff_zones, on="dropoff_location_id", how="left") \
    .select(
        "trip_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "pickup_location_id",
        "dropoff_location_id",
        "fare_amount",
        "total_amount",
        "pickup_zone",
        "pickup_borough",
        "dropoff_zone",
        "dropoff_borough",
        "kafka_offset",
        "kafka_timestamp"
    )

enriched.writeTo(silver).createOrReplace()

print("silver_taxi: created/replaced silver taxi table")
spark.sql(f"SELECT count(*) AS silver_taxi_rows FROM {silver}").show()

spark.stop()