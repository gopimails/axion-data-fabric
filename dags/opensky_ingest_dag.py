"""Airflow DAG for OpenSky aircraft state data ingestion."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG default arguments
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2024, 1, 1),
}

# Define DAG
dag = DAG(
    "opensky_ingest_dag",
    default_args=default_args,
    description="Fetch OpenSky aircraft states for Switzerland and store as Parquet",
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    catchup=False,
)


def run_ingest():
    """Execute the OpenSky ingestion pipeline."""
    from src.main import ingest_once

    success, output_path = ingest_once()
    if not success:
        raise Exception("Ingestion cycle failed")
    return output_path


# Define task
ingest_task = PythonOperator(
    task_id="opensky_ingest",
    python_callable=run_ingest,
    dag=dag,
)

# Task dependencies (none in this simple case)
ingest_task
