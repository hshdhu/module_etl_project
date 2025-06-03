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
        ["python3", os.path.join(BASE_DIR, "extract_data", "data_weather_ocean_vn.py")],
        check=True
    )

def run_load():
    subprocess.run(
        ["python3", os.path.join(BASE_DIR, "load_data", "load_data_weather_ocean_vn.py")],
        check=True
    )

with DAG(
    dag_id='etl_weather_ocean_vn',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:
    task_crawl = PythonOperator(
        task_id='crawl_weather_ocean_vn',
        python_callable=run_crawl
    )
    task_load = PythonOperator(
        task_id='load_weather_ocean_vn',
        python_callable=run_load
    )

    task_crawl >> task_load

etl_weather_ocean_vn = dag