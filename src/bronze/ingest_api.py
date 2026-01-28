import requests
import sys
import json
import os
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_spark_session():
    return SparkSession.builder \
        .appName("BreweryIngestionBronze") \
        .getOrCreate()

def ingest_api_to_bronze(base_url, storage_path):
    spark = get_spark_session()
    
    staging_dir = "data/staging/breweries"
    os.makedirs(staging_dir, exist_ok=True)
    
    staging_file = os.path.join(staging_dir, "raw_data.jsonl")
    
    if os.path.exists(staging_file):
        os.remove(staging_file)

    page = 1
    per_page = 200
    total_records = 0
    
    print(f"[{datetime.now()}] Starting ingestion process...")

    try:
        with open(staging_file, "a", encoding="utf-8") as f:
            while True:
                response = requests.get(f"{base_url}?page={page}&per_page={per_page}", timeout=15)
                response.raise_for_status()
                
                data = response.json()
                if not data:
                    break
                
                for record in data:
                    f.write(json.dumps(record) + "\n")
                
                count = len(data)
                total_records += count
                print(f"Page {page} fetched and buffered. (+{count} records)")
                page += 1

        if total_records == 0:
            print("No data retrieved.")
            return

        print(f"[{datetime.now()}] Reading from buffer into Spark...")
        
        bronze_data = spark.read \
            .option("primitivesAsString", "true") \
            .json(staging_file)

        bronze_data = bronze_data.withColumn("ingestion_timestamp", F.current_timestamp())

        bronze_data.write \
            .mode("overwrite") \
            .json(storage_path)
        
        print(f"[{datetime.now()}] Success! {total_records} records persisted to {storage_path}")

    except Exception as e:
        print(f"[{datetime.now()}] Error: {str(e)}")
        raise e
    finally:
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)

if __name__ == "__main__":
    API_URL = "https://api.openbrewerydb.org/v1/breweries"
    OUTPUT_PATH = "data/bronze/breweries"
    ingest_api_to_bronze(API_URL, OUTPUT_PATH)
