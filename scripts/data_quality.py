from sqlalchemy import create_engine
from airflow.models import Variable

def data_quality_check():
  
    db_conn_str = Variable.get("db_conn_str")

    engine = create_engine(db_conn_str)

    query = "SELECT COUNT(*) FROM sales_summary"

    with engine.connect() as connection:
        result = connection.execute(query)
        count = result.scalar()

    if count == 0:
        raise ValueError("Data quality check failed: no rows in sales_summary table")

    print(f"Data quality check passed: {count} rows found.")