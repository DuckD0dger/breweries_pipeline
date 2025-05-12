import pandas as pd
import os

def test_transform_silver_creates_partitioned_parquet():
    path = '/opt/airflow/data/silver/breweries.parquet'
    assert os.path.exists(path), "Silver layer parquet file was not created"

    df = pd.read_parquet(path)
    assert 'state' in df.columns, "Missing 'state' column in silver data"
    assert not df['state'].isnull().any(), "Null values found in 'state' column"
