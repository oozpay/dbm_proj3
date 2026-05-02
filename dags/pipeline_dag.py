from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# FIXED: Reverted Iceberg version to 1.10.0[cite: 1]
PACKAGES = "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0"


with DAG(
    'lakehouse_orchestration_dag',
    default_args=default_args,
    description='Orchestrates CDC and Taxi Streaming pipelines',
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    max_active_runs=1
) as dag:

    health_check = HttpSensor(
        task_id='health_check',
        http_conn_id='kafka_connect',
        endpoint='connectors/postgres-cdc-connector/status',
        response_check=lambda response: "RUNNING" in response.text,
        poke_interval=5,
        timeout=20
    )

    bronze_cdc = BashOperator(
        task_id='bronze_cdc',
        bash_command=f'docker exec jupyter spark-submit {PACKAGES} /home/jovyan/project/spark_jobs.py bronze_cdc'
    )

    silver_cdc = BashOperator(
        task_id='silver_cdc',
        bash_command=f'docker exec jupyter spark-submit {PACKAGES} /home/jovyan/project/spark_jobs.py silver_cdc'
    )

    bronze_taxi = BashOperator(
        task_id='bronze_taxi',
        bash_command=f'docker exec jupyter spark-submit {PACKAGES} /home/jovyan/project/spark_jobs.py bronze_taxi'
    )

    silver_taxi = BashOperator(
        task_id='silver_taxi',
        bash_command=f'docker exec jupyter spark-submit {PACKAGES} /home/jovyan/project/spark_jobs.py silver_taxi'
    )

    gold_taxi = BashOperator(
        task_id='gold_taxi',
        bash_command=f'docker exec jupyter spark-submit {PACKAGES} /home/jovyan/project/spark_jobs.py gold_taxi'
    )

    gold_efficiency = BashOperator(
        task_id='gold_efficiency',
        bash_command=f'docker exec jupyter spark-submit {PACKAGES} /home/jovyan/project/spark_jobs.py gold_assignment_efficiency'
    )

    gold_balance = BashOperator(
        task_id='gold_balance',
        bash_command=f'docker exec jupyter spark-submit {PACKAGES} /home/jovyan/project/spark_jobs.py gold_workload_balance'
    )

    # def validate_silver():
    #     pass 

    # validation = PythonOperator(
    #     task_id='validation',
    #     python_callable=validate_silver
    # )

    validation = BashOperator(
        task_id='validation',
        bash_command=f'docker exec jupyter spark-submit {PACKAGES} /home/jovyan/project/spark_jobs.py validate_silver'
    )

    # health_check >> [bronze_cdc, bronze_taxi]
    # bronze_cdc >> silver_cdc
    # bronze_taxi >> silver_taxi
    # [silver_cdc, silver_taxi] >> gold_taxi >> validation

    # Task dependency chain
    health_check >> [bronze_cdc, bronze_taxi]
    bronze_cdc >> silver_cdc
    bronze_taxi >> silver_taxi
    
    # Both Silver layers must finish before we can join them for Gold Efficiency
    [silver_cdc, silver_taxi] >> gold_taxi >> gold_efficiency >> gold_balance >> validation