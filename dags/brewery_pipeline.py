from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import sys

BASE_PATH = "/opt/airflow/data"
BRONZE_FILE = f"{BASE_PATH}/bronze/breweries_raw.json"
SILVER_OUTPUT = f"{BASE_PATH}/silver/breweries_delta"

sys.path.append('/opt/airflow')
from src.silver.refine_breweries import transform_bronze_to_silver

default_args = {
    'owner': 'Esther',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

default_args = {
    'owner': 'Esther',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def extract_bronze():
    """"Extract raw data from Open Brewery DB API to Bronze layer."""
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    
    os.makedirs(os.path.dirname(BRONZE_FILE), exist_ok=True)
    
    with open(BRONZE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Data successfully extracted to {BRONZE_FILE}")

def run_silver_transformation():
    """Call the Spark/Delta transformation logic stored in the src/ ."""
    transform_bronze_to_silver(BRONZE_FILE, SILVER_OUTPUT)

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

    transform_silver_task = PythonOperator(
        task_id='transform_silver_task',
        python_callable=run_silver_transformation
    )

    extract_task >> transform_silver_task