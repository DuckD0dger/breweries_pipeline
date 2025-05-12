import os
import pyarrow.dataset as ds
import pandas as pd

def aggregate_gold():
    silver_path = '/opt/airflow/data/silver/breweries'
    gold_dir = '/opt/airflow/data/gold'
    os.makedirs(gold_dir, exist_ok=True)

    try:
        # Leitura via PyArrow Dataset (suporta partições Hive como state=XYZ/)
        dataset = ds.dataset(silver_path, format="parquet", partitioning="hive")
        table = dataset.to_table()
        df = table.to_pandas()

        # Agregação: quantidade de cervejarias por tipo e estado
        agg_df = df.groupby(['state', 'brewery_type']).size().reset_index(name='brewery_count')

        # Salvando resultado final
        gold_path = os.path.join(gold_dir, 'brewery_aggregates.parquet')
        agg_df.to_parquet(gold_path, index=False)

        print(f"✔ Dados agregados gravados com sucesso em: {gold_path}")
    except Exception as e:
        print("❌ Erro ao ler dados particionados da Silver ou ao gerar agregação.")
        print(f"Erro: {e}")
