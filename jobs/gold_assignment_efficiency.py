from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import get_spark

spark = get_spark("gold_assignment_efficiency")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

trips = spark.table("lakehouse.taxi.silver_taxi")
drivers = spark.table("lakehouse.cdc.silver_drivers")

active_drivers = drivers.filter(F.col("is_deleted") == F.lit(False))

driver_count = active_drivers.count()

drivers_indexed = active_drivers.withColumn(
    "driver_index",
    F.row_number().over(Window.orderBy("id")) - 1
)

assigned = trips.withColumn(
    "driver_index",
    F.pmod(F.col("trip_id"), F.lit(driver_count))
).join(
    drivers_indexed,
    on="driver_index",
    how="left"
)

idle_window = Window.partitionBy("id").orderBy("pickup_datetime")

with_idle = assigned.withColumn(
    "prev_dropoff",
    F.lag("dropoff_datetime").over(idle_window)
).withColumn(
    "idle_minutes",
    F.when(
        F.col("prev_dropoff").isNotNull(),
        (F.unix_timestamp("pickup_datetime") - F.unix_timestamp("prev_dropoff")) / 60.0
    ).otherwise(0.0)
).withColumn(
    "idle_minutes",
    F.when(F.col("idle_minutes") < 0, 0.0).otherwise(F.col("idle_minutes"))
)

efficiency = with_idle.groupBy(
    F.col("id").alias("driver_id"),
    F.col("name").alias("driver_name"),
    F.col("rating").alias("current_rating"),
    F.col("city"),
    F.col("active").alias("is_active")
).agg(
    F.count("*").alias("trips_assigned"),
    F.round(F.sum("trip_distance"), 2).alias("total_distance"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.sum("total_amount") / F.sum("trip_distance"), 2).alias("avg_fare_per_mile"),
    F.round(F.sum("idle_minutes"), 2).alias("estimated_idle_time_minutes")
)

efficiency.writeTo("lakehouse.gold.gold_assignment_efficiency").createOrReplace()

print("gold_assignment_efficiency created")
spark.sql("""
SELECT *
FROM lakehouse.gold.gold_assignment_efficiency
ORDER BY avg_fare_per_mile DESC
LIMIT 20
""").show(truncate=False)

spark.stop()