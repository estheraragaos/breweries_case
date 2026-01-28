from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os

BASE_PATH = "/opt/airflow/data"

default_args = {
    'owner': 'Esther',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def extract_bronze():
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    os.makedirs(f"{BASE_PATH}/bronze", exist_ok=True)
    
    file_path = f"{BASE_PATH}/bronze/breweries_raw.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Dados extraídos com sucesso para {file_path}")

with DAG(
    dag_id='brewery_medallion_pipeline',
    default_args=default_args,
    description='Pipeline Medallion - Brewery Case',
    schedule_interval=None,
    start_date=datetime(2026, 1, 28),
    catchup=False,
    tags=['pyspark', 'medallion'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_bronze_task',
        python_callable=extract_bronze
    )