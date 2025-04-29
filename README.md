# Daily Sales ETL Pipeline

## Overview
This project builds an ETL pipeline that:
- Extracts daily sales CSV data from AWS S3
- Transforms (cleans, aggregates) the data
- Loads it into a PostgreSQL database
- Runs daily using Apache Airflow

## Project Structure
- dags/: Airflow DAGs
- scripts/: Python ETL scripts
- tests/: Unit tests
- docker-compose.yaml: Setup for Airflow + Postgres
- sales_data_sample/: Sample CSV data

## Setup Instructions
1. Create a .env file with AWS credentials.
2. Run:
   docker-compose up -d
    
3. Access Airflow UI: http://localhost:8080
    - Login: airflow / airflow
4. Trigger the etl_pipelines DAG.

## Notes
- PostgreSQL is available on localhost:5432
- Sample table created: sales_summary
