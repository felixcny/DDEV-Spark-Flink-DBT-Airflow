from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}


with DAG(
    dag_id="taxi_batch_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    ingest_taxi = BashOperator(
        task_id="ingest_taxi",
        bash_command="python /opt/airflow/scripts/ingestion_taxi.py",
    )

    ingest_taxi_zones = BashOperator(
        task_id="ingest_taxi_zones",
        bash_command="python /opt/airflow/scripts/ingestion_mappingzonestaxi.py",
    )

    transform_taxi = BashOperator(
        task_id="transform_taxi",
        bash_command="python /opt/airflow/scripts/traitement_spark_taxi.py",
    )
    
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/airflow/dbt_project && dbt run",
    )
    
    [ingest_taxi, ingest_taxi_zones] >> transform_taxi >> run_dbt