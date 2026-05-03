from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("gold_taxi")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

silver = "lakehouse.taxi.silver_taxi"
gold = "lakehouse.taxi.gold_hourly_zone"

df = spark.table(silver)

gold_df = df.groupBy(
    F.date_trunc("hour", F.col("pickup_datetime")).alias("pickup_hour"),
    F.col("pickup_borough"),
    F.col("pickup_zone")
).agg(
    F.count("*").alias("trip_count"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue")
)

gold_df.writeTo(gold).createOrReplace()

print("gold_taxi: created/replaced hourly zone aggregation")
spark.sql(f"SELECT * FROM {gold} ORDER BY pickup_hour, trip_count DESC LIMIT 20").show(truncate=False)

spark.stop()