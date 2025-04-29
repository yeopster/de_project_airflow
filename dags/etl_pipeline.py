from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

from scripts.extract import download_from_s3
from scripts.transform import transform_data
from scripts.load import load_to_postgres
from scripts.data_quality import data_quality_check

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_sales_etl',
    default_args=default_args,
    description='Daily ETL pipeline for sales data with data quality check',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 28),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_from_s3',
        python_callable=download_from_s3,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    dq_check_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
    )

    extract_task >> transform_task >> load_task >> dq_check_task