import psycopg2 # Make sure to import this at the top of your file
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys

# Spark session with full Iceberg catalog configuration[cite: 1]
spark = SparkSession.builder \
    .appName("CDC_Taxi_Pipeline") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "rest") \
    .config("spark.sql.catalog.local.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.local.warehouse", "s3://warehouse/") \
    .config("spark.sql.catalog.local.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.local.s3.path-style-access", "true") \
    .getOrCreate()

# ── Path A: CDC Pipeline ──────────────────────────────────────────────────

def process_bronze_cdc():
    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "dbserver1.public.customers") \
        .option("startingOffsets", "earliest") \
        .load()
    df.selectExpr("CAST(value AS STRING)").write.format("iceberg").mode("append").saveAsTable("local.warehouse.bronze_cdc")

# def process_silver_cdc():
#     raw_df = spark.table("local.warehouse.bronze_cdc")
    
#     updates = raw_df.select(
#         get_json_object(col("value"), "$.payload.op").alias("op"),
#         get_json_object(col("value"), "$.payload.after.id").cast("int").alias("id"),
#         get_json_object(col("value"), "$.payload.after.name").alias("name"),
#         get_json_object(col("value"), "$.payload.after.email").alias("email"),
#         get_json_object(col("value"), "$.payload.after.country").alias("country"),
#         get_json_object(col("value"), "$.payload.ts_ms").cast("long").alias("ts_ms")
#     ).filter("id IS NOT NULL")
    
#     updates.createOrReplaceTempView("cdc_updates")
    
#     spark.sql("CREATE TABLE IF NOT EXISTS local.warehouse.silver_cdc (id INT, name STRING, email STRING, country STRING, ts_ms LONG) USING iceberg")
    
#     spark.sql("""
#         MERGE INTO local.warehouse.silver_cdc t
#         USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts_ms DESC) as rn FROM cdc_updates) WHERE rn = 1) s
#         ON t.id = s.id
#         WHEN MATCHED AND s.op = 'd' THEN DELETE
#         WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email, t.country = s.country, t.ts_ms = s.ts_ms
#         WHEN NOT MATCHED AND s.op != 'd' THEN INSERT (id, name, email, country, ts_ms) VALUES (s.id, s.name, s.email, s.country, s.ts_ms)
#     """)

def process_silver_cdc():
    raw_df = spark.table("local.warehouse.bronze_cdc")
    
    # Extract the lsn to handle millisecond tie-breakers
    updates = raw_df.select(
        get_json_object(col("value"), "$.payload.op").alias("op"),
        coalesce(
            get_json_object(col("value"), "$.payload.after.id").cast("int"),
            get_json_object(col("value"), "$.payload.before.id").cast("int")
        ).alias("id"),
        get_json_object(col("value"), "$.payload.after.name").alias("name"),
        get_json_object(col("value"), "$.payload.after.email").alias("email"),
        get_json_object(col("value"), "$.payload.after.country").alias("country"),
        get_json_object(col("value"), "$.payload.ts_ms").cast("long").alias("ts_ms"),
        get_json_object(col("value"), "$.payload.source.lsn").cast("long").alias("lsn")
    ).filter("id IS NOT NULL")
    
    updates.createOrReplaceTempView("cdc_updates")
    
    spark.sql("CREATE TABLE IF NOT EXISTS local.warehouse.silver_cdc (id INT, name STRING, email STRING, country STRING, ts_ms LONG) USING iceberg")
    
    # FIXED: Added 'lsn DESC' as the tie-breaker in the ORDER BY clause
    spark.sql("""
        MERGE INTO local.warehouse.silver_cdc t
        USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts_ms DESC, lsn DESC) as rn FROM cdc_updates) WHERE rn = 1) s
        ON t.id = s.id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email, t.country = s.country, t.ts_ms = s.ts_ms
        WHEN NOT MATCHED AND s.op != 'd' THEN INSERT (id, name, email, country, ts_ms) VALUES (s.id, s.name, s.email, s.country, s.ts_ms)
    """)

# def process_silver_cdc():
#     raw_df = spark.table("local.warehouse.bronze_cdc")
    
#     # Extract data using the 'ts_ms' alias to match your existing table schema
#     updates = raw_df.select(
#         get_json_object(col("value"), "$.payload.op").alias("op"),
#         coalesce(
#             get_json_object(col("value"), "$.payload.after.id").cast("int"),
#             get_json_object(col("value"), "$.payload.before.id").cast("int")
#         ).alias("id"),
#         get_json_object(col("value"), "$.payload.after.name").alias("name"),
#         get_json_object(col("value"), "$.payload.after.email").alias("email"),
#         get_json_object(col("value"), "$.payload.after.country").alias("country"),
#         # Map source timestamp back to 'ts_ms' so it matches your current Iceberg table
#         get_json_object(col("value"), "$.payload.source.ts_ms").cast("long").alias("ts_ms"),
#         get_json_object(col("value"), "$.payload.source.lsn").alias("lsn")
#     ).filter("id IS NOT NULL")
    
#     updates.createOrReplaceTempView("cdc_updates")
    
#     # This ensures the table exists with the correct base columns
#     spark.sql("CREATE TABLE IF NOT EXISTS local.warehouse.silver_cdc (id INT, name STRING, email STRING, country STRING, ts_ms LONG) USING iceberg")
    
#     # Use ts_ms for sorting and matching to align with the physical table
#     spark.sql("""
#         MERGE INTO local.warehouse.silver_cdc t
#         USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts_ms DESC, lsn DESC) as rn FROM cdc_updates) WHERE rn = 1) s
#         ON t.id = s.id
#         WHEN MATCHED AND s.op = 'd' THEN DELETE
#         WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email, t.country = s.country, t.ts_ms = s.ts_ms
#         WHEN NOT MATCHED AND s.op != 'd' THEN INSERT (id, name, email, country, ts_ms) VALUES (s.id, s.name, s.email, s.country, s.ts_ms)
#     """)

# ── Path B: Taxi Pipeline ─────────────────────────────────────────────────

def process_bronze_taxi():
    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "taxi-trips") \
        .option("startingOffsets", "earliest") \
        .load()
    df.selectExpr("CAST(value AS STRING)").write.format("iceberg").mode("append").saveAsTable("local.warehouse.bronze_taxi")

def process_silver_taxi():
    raw = spark.table("local.warehouse.bronze_taxi")
    cleaned = raw.select(
        get_json_object(col("value"), "$.VendorID").alias("vendor_id"),
        get_json_object(col("value"), "$.fare_amount").cast("double").alias("fare"),
        get_json_object(col("value"), "$.tpep_pickup_datetime").alias("pickup_time")
    ).filter("fare > 0")
    cleaned.write.format("iceberg").mode("append").saveAsTable("local.warehouse.silver_taxi")

def process_gold_taxi():
    silver = spark.table("local.warehouse.silver_taxi")
    gold = silver.groupBy("vendor_id").agg(sum("fare").alias("total_revenue"), count("*").alias("trip_count"))
    gold.write.format("iceberg").mode("overwrite").saveAsTable("local.warehouse.gold_taxi")


# custom scenario
def process_gold_assignment_efficiency():
    """Builds driver-level efficiency metrics combining Taxi and CDC data."""
    
    taxi_df = spark.table("local.warehouse.bronze_taxi")
    silver_cdc = spark.table("local.warehouse.silver_cdc")
    
    # 1. Simulate the Driver Assignment: MOD(trip_id, driver_count)
    active_drivers = silver_cdc.count()
    if active_drivers == 0: active_drivers = 1 # Prevent modulo by zero
    
    driver_mapping = silver_cdc.withColumn("driver_index", row_number().over(Window.orderBy("id")) - 1)
    
    # FIXED: Added 'T' to the timestamp format strings to correctly parse ISO 8601 dates
    parsed_taxi = taxi_df.select(
        get_json_object(col("value"), "$.trip_distance").cast("double").alias("distance"),
        get_json_object(col("value"), "$.fare_amount").cast("double").alias("fare"),
        to_timestamp(get_json_object(col("value"), "$.tpep_pickup_datetime"), "yyyy-MM-dd'T'HH:mm:ss").alias("pickup"),
        to_timestamp(get_json_object(col("value"), "$.tpep_dropoff_datetime"), "yyyy-MM-dd'T'HH:mm:ss").alias("dropoff")
    )
    
    # NOW apply the synthetic trip_id using the parsed pickup time
    assigned_trips_raw = parsed_taxi.withColumn(
        "trip_id", row_number().over(Window.orderBy("pickup"))
    ).withColumn(
        "assigned_index", col("trip_id") % lit(active_drivers)
    )
    
    # Join with CDC data to get the actual driver ID and rating
    assigned_trips = assigned_trips_raw.join(
        driver_mapping, 
        assigned_trips_raw.assigned_index == driver_mapping.driver_index, 
        "left"
    )

    # 2. Calculate Idle Time using Window functions
    window_spec = Window.partitionBy("id").orderBy("pickup")
    
    trips_with_lag = assigned_trips.withColumn(
        "prev_dropoff", 
        lag("dropoff").over(window_spec)
    )
    
    # Idle time in seconds
    trips_with_idle = trips_with_lag.withColumn(
        "idle_seconds",
        when(col("prev_dropoff").isNotNull() & (col("pickup") > col("prev_dropoff")),
             unix_timestamp("pickup") - unix_timestamp("prev_dropoff"))
        .otherwise(0)
    )

    # 3. Aggregate per-driver metrics
    efficiency = trips_with_idle.groupBy(
        col("id").alias("driver_id"), 
        col("country").alias("driver_rating") 
    ).agg(
        count("*").alias("trips_assigned"),
        sum("distance").alias("total_distance"),
        sum("fare").alias("total_revenue"),
        sum("idle_seconds").alias("total_idle_seconds")
    ).withColumn(
        "avg_fare_per_mile",
        when(col("total_distance") > 0, col("total_revenue") / col("total_distance")).otherwise(0)
    ).withColumn(
        "is_active", lit(True) 
    )

    efficiency.write.format("iceberg").mode("overwrite").saveAsTable("local.warehouse.gold_assignment_efficiency")


def process_gold_workload_balance():
    """Builds a system-wide summary of driver workload distribution."""
    
    efficiency = spark.table("local.warehouse.gold_assignment_efficiency")
    
    # Calculate global averages
    system_stats = efficiency.select(
        avg("trips_assigned").alias("avg_trips"),
        stddev("trips_assigned").alias("stddev_trips"),
        max("trips_assigned").alias("max_trips"),
        min("trips_assigned").alias("min_trips")
    ).collect()[0]
    
    avg_trips = system_stats["avg_trips"] or 0
    
    # Flag overloaded drivers
    balance_details = efficiency.withColumn(
        "is_overloaded", 
        when(col("trips_assigned") > (avg_trips * 2), True).otherwise(False)
    )
    
    balance_details.write.format("iceberg").mode("overwrite").saveAsTable("local.warehouse.gold_workload_balance")

# validation
def process_validate_silver():
    # 1. Get the row count from the Iceberg Silver table
    silver_count_row = spark.sql("SELECT COUNT(*) as cnt FROM local.warehouse.silver_cdc").collect()
    silver_count = silver_count_row[0]['cnt']

    # 2. Get the row count from PostgreSQL
    # (Connect directly to the source database)
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="sourcedb",
        user="cdc_user",
        password="admin"
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM customers;")
    pg_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    # 3. Compare and validate
    print(f"--- VALIDATION RESULTS ---")
    print(f"PostgreSQL customers count: {pg_count}")
    print(f"Iceberg silver_cdc count: {silver_count}")
    
    if pg_count != silver_count:
        # Raising an exception will cause the Airflow task to fail and turn red
        raise ValueError(f"VALIDATION FAILED: Source has {pg_count} rows, but Silver has {silver_count} rows.")
    
    print("VALIDATION SUCCESSFUL: Silver table matches PostgreSQL source.")

if __name__ == "__main__":
    task = sys.argv[1]
    if task == "validate_silver":
        process_validate_silver()
    else:
        globals()[f"process_{task}"]()