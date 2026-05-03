from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import get_spark

spark = get_spark("silver-cdc")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")

# ---------- customers staging ----------
c = spark.table("lakehouse.cdc.bronze_customers").filter(F.col("op").isNotNull())

customers = c.select(
    F.coalesce(
        F.get_json_object("after_json", "$.id").cast("int"),
        F.get_json_object("before_json", "$.id").cast("int")
    ).alias("id"),
    F.get_json_object("after_json", "$.name").alias("name"),
    F.get_json_object("after_json", "$.email").alias("email"),
    F.get_json_object("after_json", "$.country").alias("country"),
    F.get_json_object("after_json", "$.created_at").alias("created_at"),
    F.col("op"),
    F.col("ts_ms"),
    F.col("kafka_offset")
)

w = Window.partitionBy("id").orderBy(F.col("ts_ms").desc(), F.col("kafka_offset").desc())

latest_customers = (
    customers.withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)

latest_customers.writeTo("lakehouse.cdc.stage_latest_customers").createOrReplace()

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.cdc.silver_customers (
    id INT,
    name STRING,
    email STRING,
    country STRING,
    created_at STRING,
    last_op STRING,
    last_updated_ms BIGINT
)
USING iceberg
""")

spark.sql("""
MERGE INTO lakehouse.cdc.silver_customers t
USING lakehouse.cdc.stage_latest_customers s
ON t.id = s.id
WHEN MATCHED AND s.op = 'd' THEN DELETE
WHEN MATCHED AND s.op IN ('c','u','r') THEN UPDATE SET
    name = s.name,
    email = s.email,
    country = s.country,
    created_at = s.created_at,
    last_op = s.op,
    last_updated_ms = s.ts_ms
WHEN NOT MATCHED AND s.op IN ('c','u','r') THEN INSERT
    (id, name, email, country, created_at, last_op, last_updated_ms)
    VALUES (s.id, s.name, s.email, s.country, s.created_at, s.op, s.ts_ms)
""")

print("Merged customers")

# ---------- drivers staging ----------
d = spark.table("lakehouse.cdc.bronze_drivers").filter(F.col("op").isNotNull())

drivers = d.select(
    F.coalesce(
        F.get_json_object("after_json", "$.id").cast("int"),
        F.get_json_object("before_json", "$.id").cast("int")
    ).alias("id"),
    F.get_json_object("after_json", "$.name").alias("name"),
    F.get_json_object("after_json", "$.license_number").alias("license_number"),
    F.get_json_object("after_json", "$.rating").cast("double").alias("rating"),
    F.get_json_object("after_json", "$.city").alias("city"),
    F.get_json_object("after_json", "$.active").cast("boolean").alias("active"),
    F.get_json_object("after_json", "$.created_at").alias("created_at"),
    F.col("op"),
    F.col("ts_ms"),
    F.col("kafka_offset")
)

w2 = Window.partitionBy("id").orderBy(F.col("ts_ms").desc(), F.col("kafka_offset").desc())

latest_drivers = (
    drivers.withColumn("rn", F.row_number().over(w2))
    .filter("rn = 1")
    .drop("rn")
)

latest_drivers.writeTo("lakehouse.cdc.stage_latest_drivers").createOrReplace()

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.cdc.silver_drivers (
    id INT,
    name STRING,
    license_number STRING,
    rating DOUBLE,
    city STRING,
    active BOOLEAN,
    created_at STRING,
    is_deleted BOOLEAN,
    last_op STRING,
    last_updated_ms BIGINT
)
USING iceberg
""")

spark.sql("""
MERGE INTO lakehouse.cdc.silver_drivers t
USING lakehouse.cdc.stage_latest_drivers s
ON t.id = s.id
WHEN MATCHED AND s.op = 'd' THEN UPDATE SET
    active = false,
    is_deleted = true,
    last_op = s.op,
    last_updated_ms = s.ts_ms
WHEN MATCHED AND s.op IN ('c','u','r') THEN UPDATE SET
    name = s.name,
    license_number = s.license_number,
    rating = s.rating,
    city = s.city,
    active = s.active,
    created_at = s.created_at,
    is_deleted = false,
    last_op = s.op,
    last_updated_ms = s.ts_ms
WHEN NOT MATCHED AND s.op IN ('c','u','r') THEN INSERT
    (id, name, license_number, rating, city, active, created_at, is_deleted, last_op, last_updated_ms)
    VALUES (s.id, s.name, s.license_number, s.rating, s.city, s.active, s.created_at, false, s.op, s.ts_ms)
""")

print("Merged drivers")

spark.sql("SELECT COUNT(*) AS silver_customers FROM lakehouse.cdc.silver_customers").show()
spark.sql("SELECT COUNT(*) AS silver_drivers FROM lakehouse.cdc.silver_drivers").show()

spark.stop()
