import pandas as pd

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