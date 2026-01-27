import requests
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

def get_spark_session():
    """Creates or retrieves a Spark session."""
    return SparkSession.builder \
        .appName("BreweryIngestionBronze") \
        .getOrCreate()

def ingest_api_to_bronze(base_url, storage_path):
    """
    Fetches data from Open Brewery DB API and persists it in JSON format 
    within the Bronze Layer of the data lake.
    """
    spark = get_spark_session()
    
    ingestion_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True)
    ])

    all_data = []
    page = 1
    per_page = 200 # as per API documentation limits
    
    print(f"[{datetime.now()}] Starting ingestion process...")

    try:
        while True:
            response = requests.get(f"{base_url}?page={page}&per_page={per_page}", timeout=15)
            response.raise_for_status()
            
            data = response.json()
            if not data:
                break
            
            all_data.extend(data)
            print(f"Page {page} successfully fetched. Records found: {len(data)}")
            page += 1

        if not all_data:
            print("No data retrieved from API.")
            return

        bronze_data = spark.createDataFrame(all_data, schema=ingestion_schema)

        bronze_data = bronze_data.withColumn("ingestion_timestamp", F.current_timestamp())

        bronze_data.write \
            .mode("overwrite") \
            .json(storage_path)
        
        print(f"[{datetime.now()}] Success! Data persisted to {storage_path}")
        print(f"[{datetime.now()}] Bronze layer populated with {bronze_data.count()} records.")

    except Exception as e:
        print(f"[{datetime.now()}] Error during ingestion: {str(e)}")
        raise e

if __name__ == "__main__":
    # Basic Configuration
    API_URL = "https://api.openbrewerydb.org/v1/breweries"
    OUTPUT_PATH = "data/bronze/breweries"

    try:
        ingest_api_to_bronze(API_URL, OUTPUT_PATH)
    except Exception:
        sys.exit(1)