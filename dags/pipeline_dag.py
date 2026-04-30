from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import requests

default_args = {
    "owner": "your-group",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

def check_connector():
    r = requests.get("http://connect:8083/connectors/cdc-connector/status")
    status = r.json()["connector"]["state"]
    assert status == "RUNNING", f"Connector is {status}, not RUNNING"

with DAG(
    dag_id="project3_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="*/15 * * * *", 
    catchup=False,
    tags=["project3", "lakehouse"],
) as dag:

    # --- Health Check ---
    health_check = PythonOperator(
        task_id="check_debezium_health",
        python_callable=check_connector,
    )

    # --- CDC Branch ---
    bronze_cdc = SparkSubmitOperator(
        task_id="bronze_cdc",
        application="/opt/airflow/scripts/cdc_bronze.py", # Update path as needed
        conn_id="spark_default"
    )

    silver_cdc = SparkSubmitOperator(
        task_id="silver_cdc",
        application="/opt/airflow/scripts/cdc_silver.py", # Update path as needed
        conn_id="spark_default"
    )

    # --- Taxi Branch ---
    bronze_taxi = SparkSubmitOperator(
        task_id="bronze_taxi",
        application="/opt/airflow/scripts/taxi_bronze.py", # Update path as needed
        conn_id="spark_default"
    )

    silver_taxi = SparkSubmitOperator(
        task_id="silver_taxi",
        application="/opt/airflow/scripts/taxi_silver.py", # Update path as needed
        conn_id="spark_default"
    )

    # --- Orchestration / Dependencies ---
    health_check >> bronze_cdc >> silver_cdc
    bronze_taxi >> silver_taxi