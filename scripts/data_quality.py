def data_quality_check():
    df = pd.read_csv('/tmp/sales_data_transformed.csv')

    if df.empty:
        raise ValueError("Data quality check failed: DataFrame is empty.")
    if df.isnull().sum().any():
        raise ValueError("Data quality check failed: Null values found.")

    print("Data quality check passed.")
