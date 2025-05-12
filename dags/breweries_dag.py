from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append('/opt/airflow/scripts')

from fetch_breweries import fetch_all_breweries
from transform_silver import transform_silver
from aggregate_gold import aggregate_gold

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag = DAG(
    dag_id='breweries_pipeline',
    default_args=default_args,
    description='ETL pipeline for brewery data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

t1 = PythonOperator(
    task_id='fetch_raw_data',
    python_callable=fetch_all_breweries,
    dag=dag
)

t2 = PythonOperator(
    task_id='transform_to_silver',
    python_callable=transform_silver,
    dag=dag
)

t3 = PythonOperator(
    task_id='aggregate_to_gold',
    python_callable=aggregate_gold,
    dag=dag
)

t1 >> t2 >> t3
