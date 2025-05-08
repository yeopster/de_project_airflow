# Daily Sales ETL Pipeline with Airflow Dags, PostgreSQL and Docker

## Summary
This project builds an ETL pipeline that:
- Extracts daily sales CSV data from AWS S3
- Transforms the data with cleaning and aggregating
- Data quality checks
- Loads it into a PostgreSQL database with Pg4 Admin
- Runs daily using Apache Airflow

## Project Structure
- dags/: Airflow DAGs with Built-in ETL scripts
- scripts/: Python ETL scripts
- tests/: Unit tests
- docker-compose.yaml: Setup for Airflow + Postgres + Pg4 Admin
- sales_data_sample/: Sample CSV data

## Setup Instructions
1. AWS Credentials is hardcode
2. Run:
   docker-compose up -d
3. Access Airflow UI: http://localhost:8080
    - Login: airflow / airflow
4. Trigger the etl_pipelines DAG.
5. Login Pg4 Admin http://localhost:5050
    - Login: admin@admin.com / root

## Important Notes
- PostgreSQL is available on localhost:5432
- Sample table created: sales_summary
- For testing run the code in the terminal "airflow dags test sales_etl_pipeline 2025-04-27"
- AWS Access Code is altered for security reasons, if need real one can contact me
