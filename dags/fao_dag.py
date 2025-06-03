from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run_crawl():
    subprocess.run(
        ["python3", os.path.join(BASE_DIR, "extract_data", "crawl_data_fao.py")],
        check=True
    )

def run_transform():
    subprocess.run(
        ["python3", os.path.join(BASE_DIR, "transform", "transform_fao_long.py")],
        check=True
    )

def run_load():
    subprocess.run(
        ["python3", os.path.join(BASE_DIR, "load_data", "load_fao_long.py")],
        check=True
    )

with DAG(
    dag_id='etl_fao_data',
    default_args=default_args,
    schedule='@weekly',
    catchup=False
) as dag:
    task_crawl = PythonOperator(
        task_id='crawl_data_fao',
        python_callable=run_crawl
    )
    task_transform = PythonOperator(
        task_id='transform_fao_data',
        python_callable=run_transform
    )
    task_load = PythonOperator(
        task_id='load_fao_data',
        python_callable=run_load
    )

    task_crawl >> task_transform >> task_load
etl_fao_data = dag