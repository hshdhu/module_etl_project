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

def run_extract():
    subprocess.run(
        ["python3", os.path.join(BASE_DIR, "extract_data", "crawl_sanluong_thuysan.py")],
        check=True
    )

def run_transform():
    subprocess.run(
        ["python3", os.path.join(BASE_DIR, "transform", "transform_san_luong_thuy_san.py")],
        check=True
    )

def run_load():
    subprocess.run(
        ["python3", os.path.join(BASE_DIR, "load_data", "load_san_luong_thuy_san.py")],
        check=True
    )

with DAG(
    dag_id='san_luong_thuy_san',
    default_args=default_args,
    schedule='@weekly',
    catchup=False
) as dag:
    task_extract = PythonOperator(
        task_id='extract_san_luong_thuy_san',
        python_callable=run_extract
    )
    task_transform = PythonOperator(
        task_id='transform_san_luong_thuy_san',
        python_callable=run_transform
    )
    task_load = PythonOperator(
        task_id='load_san_luong_thuy_san',
        python_callable=run_load
    )

    task_extract >> task_transform >> task_load

data_global_ocean_observation = dag