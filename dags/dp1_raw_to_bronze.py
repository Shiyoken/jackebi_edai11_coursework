from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2026, 10, 1),
}

with DAG(
    dag_id ="dp1_raw_to_bronze",
    default_args = default_args,
    description="DP1: Ingest Raw Landing to Lakehouse Bronze and Validate Quality Gate",
    schedule_interval="@daily",
    catchup=False,
    tags=["lakehouse","bronze","dp1"],
) as dag:
    # Ingest from Raw Landing to Bronze Delta 
    ingest_task = BashOperator(
        task_id="ingest_raw_to_bronze",
        bash_command=(
            "docker exec food_delivery_spark_master "
            "/opt/spark/bin/spark-submit /opt/spark-apps/02_raw_to_bronze.py --stage ingest"
        ),
    )
    validate_task = BashOperator(
        task_id="valid_bronze_quality_gate",
        bash_command=(
            "docker exec food_delivery_spark_master "
            "/opt/spark/bin/spark-submit /opt/spark-apps/02_raw_to_bronze.py --stage validate"
        ),
    )

    # Dependencies: ingest -> validate
    ingest_task >> validate_task 
    