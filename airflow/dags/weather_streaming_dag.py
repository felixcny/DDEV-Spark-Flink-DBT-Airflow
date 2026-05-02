from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="weather_streaming_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    ingest_weather = BashOperator(
        task_id="ingest_weather",
        bash_command="python /opt/airflow/scripts/ingest_weather.py",
    )

    transform_weather = BashOperator(
        task_id="transform_weather",
        bash_command="python /opt/airflow/spark_jobs/transform_weather.py",
    )

    ingest_weather >> transform_weather