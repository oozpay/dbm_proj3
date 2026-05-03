from pyspark.sql import SparkSession

PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"
)

def get_spark(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .getOrCreate()
    )
