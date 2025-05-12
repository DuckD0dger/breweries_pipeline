import os
import json
import pandas as pd
import pyarrow.dataset as ds

def transform_silver():
    # Caminhos
    bronze_path = '/opt/airflow/data/bronze/breweries_raw.json'
    silver_dir = '/opt/airflow/data/silver/breweries'

    os.makedirs(silver_dir, exist_ok=True)

    # Leitura do JSON
    with open(bronze_path, 'r') as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Escrita como Parquet particionado por 'state'
    try:
        df.to_parquet(silver_dir, partition_cols=['state'], index=False)
        print(f"Parquet particionado gravado em: {silver_dir}")
    except Exception as write_err:
        print(f" Erro ao gravar Parquet: {write_err}")
        return

    try:
        dataset = ds.dataset(silver_dir, format="parquet", partitioning="hive")
        table = dataset.to_table()
        test_df = table.to_pandas()
        print(f"✔ Validação de leitura OK: {len(test_df)} registros.")
    except Exception as read_err:
        print("Falha na validação da leitura dos Parquets particionados.")
        print(f"Erro: {read_err}")
