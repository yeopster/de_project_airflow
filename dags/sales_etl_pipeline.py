from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import boto3
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

AWS_ACCESS_KEY_ID = 'AKIAS74TMDCP3GJNPR6'
AWS_SECRET_ACCESS_KEY = 'YOUR_SECRET_KEY'
AWS_REGION = 'ap-southeast-2'
BUCKET_NAME = 'salesdatafaris22'
S3_KEY = 'sales_data_sample.csv'

def download_from_s3():
    s3 = boto3.client('s3',
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_REGION)

    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_KEY)
    objects = response.get('Contents', [])

    if not objects:
        raise Exception("No files found in S3 bucket with prefix.")

    latest_file = sorted(objects, key=lambda obj: obj['LastModified'])[-1]
    file_key = latest_file['Key']

    local_filename = '/tmp/sales_data.csv'
    s3.download_file(BUCKET_NAME, file_key, local_filename)

    print(f"Downloaded {file_key} to {local_filename}")

def transform_data():
    input_path = '/tmp/sales_data.csv'
    output_path = '/tmp/sales_data_transformed.csv'

    df = pd.read_csv(input_path)
    df = df.dropna()
    df['order_date'] = pd.to_datetime(df['order_date'])

    agg_df = df.groupby('product_category').agg(
        total_sales=('amount', 'sum'),
        total_orders=('order_id', 'count')
    ).reset_index()

    agg_df.to_csv(output_path, index=False)
    print(f"Transformed data saved to {output_path}")

def data_quality_check():
    df = pd.read_csv('/tmp/sales_data_transformed.csv')

    if df.empty:
        raise ValueError("Data quality check failed: DataFrame is empty.")
    if df.isnull().sum().any():
        raise ValueError("Data quality check failed: Null values found.")

    print("Data quality check passed.")

def load_to_postgres():
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'postgres'
    db_port = '5432'
    db_name = 'airflow'

    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    output_path = '/tmp/sales_data_transformed.csv'
    df = pd.read_csv(output_path)

    df.to_sql('sales_summary', engine, if_exists='replace', index=False)

    with engine.connect() as conn:
        result = conn.execute('SELECT COUNT(*) FROM sales_summary')
        count = result.scalar()

        if count == 0:
            raise Exception("Data load failed! No records found.")

    print("Data successfully loaded into PostgreSQL.")

with DAG(
    dag_id='sales_etl_pipeline',
    default_args=default_args,
    description='ETL without dotenv: S3 to Postgres',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 27),
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    quality_check_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    extract_task >> transform_task >> quality_check_task >> load_task
