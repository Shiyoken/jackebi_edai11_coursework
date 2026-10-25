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
    dag_id="dp2_bronze_to_gold",
    default_args=default_args,
    description="DP2: Bronze -> Silver Cleansing -> Gold Star Schema & Quality Gate",
    schedule_interval="@daily",
    catchup=False,
    tags=["lakehouse", "silver", "gold", "dp2"],
) as dag:
    # 1. Clean Bronze -> Silver
    clean_silver_task = BashOperator(
        task_id="clean_bronze_to_silver",
        bash_command=(
            "docker exec food_delivery_spark_master "
            "/opt/spark/bin/spark-submit /opt/spark-apps/03_bronze_to_silver.py --stage ingest"
        ),
    )

    # 2. Silver Quality Gate
    validate_silver_task = BashOperator(
        task_id="validate_silver_quality_gate",
        bash_command=(
            "docker exec food_delivery_spark_master "
            "/opt/spark/bin/spark-submit /opt/spark-apps/03_bronze_to_silver.py --stage validate"
        ),
    )

    # 3. Build Gold Star Schema & OBT
    build_gold_task = BashOperator(
        task_id="build_silver_to_gold",
        bash_command=(
            "docker exec food_delivery_spark_master "
            "/opt/spark/bin/spark-submit /opt/spark-apps/04_silver_to_gold.py --stage ingest"
        ),
    )

    # 4. Gold Quality Gate
    validate_gold_task = BashOperator(
        task_id="validate_gold_quality_gate",
        bash_command=(
            "docker exec food_delivery_spark_master "
            "/opt/spark/bin/spark-submit /opt/spark-apps/04_silver_to_gold.py --stage validate"
        ),
    )

    # 5. Sync Gold to PostgreSQL DWH & Optimize Storage
    sync_dwh_task = BashOperator(
        task_id="sync_gold_to_postgres_dwh",
        bash_command=(
            "docker exec food_delivery_spark_master "
            "/opt/spark/bin/spark-submit /opt/spark-apps/05_optimize_to_storage.py"
        ),
    )

    # Dependency Pipeline
    clean_silver_task >> validate_silver_task >> build_gold_task >> validate_gold_task >> sync_dwh_task