from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

def run_extract():
    subprocess.run(
        ["python3", "extract_data/crawl_data_fao.py"],
        check=True
    )

def run_transform():
    subprocess.run(
        ["python3", "transform/transform_fao_long.py"],
        check=True
    )

def run_load():
    subprocess.run(
        ["python3", "load_data/load_fao_long.py"],
        check=True
    )

with DAG(
    dag_id='data_global_ocean_observation',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False
) as dag:
    task_extract = PythonOperator(
        task_id='extract_fao_data',
        python_callable=run_extract
    )
    task_transform = PythonOperator(
        task_id='transform_fao_data',
        python_callable=run_transform
    )
    task_load = PythonOperator(
        task_id='load_fao_data',
        python_callable=run_load
    )

    task_extract >> task_transform >> task_load
    