from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import io

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_from_s3(**kwargs):
    s3 = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'your-s3-bucket-name'
    key = 'path/to/your-data.csv'
    
    file_obj = s3.get_key(key, bucket_name).get()['Body']
    df = pd.read_csv(io.BytesIO(file_obj.read()))
    
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())

def transform_data(**kwargs):
    df = pd.read_json(kwargs['ti'].xcom_pull(key='raw_data'))
    
    # Example transformation: drop rows with missing values
    df_clean = df.dropna()
    
    kwargs['ti'].xcom_push(key='clean_data', value=df_clean.to_json())

def quality_check(**kwargs):
    df = pd.read_json(kwargs['ti'].xcom_pull(key='clean_data'))
    
    if df.empty:
        raise ValueError("Data quality check failed: DataFrame is empty.")
    
    if df.isnull().sum().any():
        raise ValueError("Data quality check failed: Contains null values.")
    
    print("Data quality check passed.")

def load_to_postgres(**kwargs):
    df = pd.read_json(kwargs['ti'].xcom_pull(key='clean_data'))
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    df.to_sql('your_table_name', engine, if_exists='replace', index=False)

with DAG(
    dag_id='s3_to_postgres_etl',
    default_args=default_args,
    description='ETL from S3 to Postgres with transformation and quality check',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract_from_s3',
        python_callable=extract_from_s3,
        provide_context=True
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    quality = PythonOperator(
        task_id='quality_check',
        python_callable=quality_check,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True
    )

    extract >> transform >> quality >> load
