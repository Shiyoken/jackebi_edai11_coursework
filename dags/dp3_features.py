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
    dag_id="dp3_features",
    default_args=default_args,
    description="DP3: Compute Offline Feature Table and Validate Quality Gate",
    schedule_interval="@daily",
    catchup=False,
    tags=["features", "ml_platform", "feast", "dp3"],
) as dag:

    # 1. Compute Feature Table & Sync to Postgres DWH
    compute_features_task = BashOperator(
        task_id="compute_offline_features",
        bash_command=(
            "docker exec food_delivery_spark_master "
            "/opt/spark/bin/spark-submit "
            "/opt/spark-apps/06_gold_to_feature.py --stage ingest"
        ),
    )

    # 2. DP3 Quality Gate 
    validate_features_task = BashOperator(
        task_id="validate_features_quality_gate",
        bash_command=(
            "docker exec food_delivery_spark_master "
            "/opt/spark/bin/spark-submit "
            "/opt/spark-apps/06_gold_to_feature.py --stage validate"
        ),
    )
    # 3. Emit DataHub Lineage & Verified Assertions (runs ONLY if validate_features_task passes!)
    emit_datahub_task = BashOperator(
        task_id="emit_datahub_metadata",
        bash_command=(
            "python /opt/airflow/scripts/emit_datahub_lineage.py --gms-url http://datahub-gms:8080"
        ),
    )

    # Dependency: Compute first, then Validate
    compute_features_task >> validate_features_task >> emit_datahub_task