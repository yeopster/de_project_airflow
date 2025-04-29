import pandas as pd
from sqlalchemy import create_engine

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

    # Data Quality Check
    with engine.connect() as conn:
        result = conn.execute('SELECT COUNT(*) FROM sales_summary')
        count = result.scalar()

        if count == 0:
            raise Exception("Data load failed! No records found.")

    print("Data successfully loaded into PostgreSQL.")