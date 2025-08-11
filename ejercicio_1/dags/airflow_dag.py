from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.pipeline import main as run_pipeline

default_args = {
    "owner": "antony",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "data_pipeline",
    default_args=default_args,
    description="Pipeline ETL hacia PostgreSQL",
    schedule_interval="0 6 * * *",  # todos los días a las 06:00
    start_date=datetime(2025, 8, 1),
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id="run_full_pipeline",
        python_callable=run_pipeline
    )

    etl_task