from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
with DAG(
    'dags_airflow_simple',
    default_args=default_args,
    description='A simple DAG that runs daily at 7:00 AM',
    schedule_interval='0 7 * * *',  # every day at 7:00 AM
    start_date=datetime(2025, 4, 28),
    catchup=False,
    tags=['simple'],
) as dag:

    start = EmptyOperator(
        task_id='start'
    )