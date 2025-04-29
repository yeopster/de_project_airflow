import pandas as pd
from scripts.transform import transform_data

def test_transform_data(tmp_path):
    
    df = pd.DataFrame({
        'order_id': [1, 2, 3],
        'order_date': ['2025-04-25', '2025-04-25', '2025-04-25'],
        'product_category': ['Electronics', 'Clothing', 'Electronics'],
        'amount': [300, 150, 500]
    })

    input_path = tmp_path / "sales_data.csv"
    output_path = tmp_path / "sales_data_transformed.csv"

    df.to_csv(input_path, index=False)

   
    transform_data()

    transformed_df = pd.read_csv(output_path)

    assert 'product_category' in transformed_df.columns
    assert 'total_sales' in transformed_df.columns
    assert 'total_orders' in transformed_df.columns
    assert len(transformed_df) > 0