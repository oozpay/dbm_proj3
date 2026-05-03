from pyspark.sql import functions as F
from common import get_spark

spark = get_spark("gold_workload_balance")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

eff = spark.table("lakehouse.gold.gold_assignment_efficiency")

stats = eff.agg(
    F.round(F.stddev("trips_assigned"), 2).alias("stddev_trips_across_drivers"),
    F.max("trips_assigned").alias("max_driver_trips"),
    F.min("trips_assigned").alias("min_driver_trips"),
    F.round(F.avg("trips_assigned"), 2).alias("avg_driver_trips")
)

summary = stats.withColumn(
    "max_to_min_trip_ratio",
    F.round(F.col("max_driver_trips") / F.col("min_driver_trips"), 2)
)

overloaded = eff.filter(
    F.col("trips_assigned") > 2 * eff.agg(F.avg("trips_assigned")).collect()[0][0]
).select(
    F.collect_list("driver_id").alias("drivers_more_than_2x_average")
)

result = summary.crossJoin(overloaded)

result.writeTo("lakehouse.gold.gold_workload_balance").createOrReplace()

print("gold_workload_balance created")
spark.sql("SELECT * FROM lakehouse.gold.gold_workload_balance").show(truncate=False)

spark.stop()