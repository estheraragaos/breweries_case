import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_spark_session():
    return SparkSession.builder \
        .appName("BreweryGoldDelta") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED") \
        .getOrCreate()

def transform_silver_to_gold(input_path, output_path):
    spark = get_spark_session()
    sys.stdout.write(f"[{datetime.now()}] Aggregating brewery data by type and location for Gold layer...\n")
    
    try:
        silver_data = spark.read.format("delta").load(input_path)
        
        gold_data = silver_data.groupBy(F.initcap("country").alias("country"), "brewery_type") \
                               .agg(F.count("id").alias("brewery_count")) \
                               .orderBy("country", F.desc("brewery_count"))
        
        gold_data.write \
            .format("delta") \
            .mode("overwrite") \
            .save(output_path)
        
        total_rows = gold_data.count()
        sys.stdout.write(f"[{datetime.now()}] Gold layer finalized. {total_rows} aggregated records successfully persisted to Delta Lake.\n")

    except Exception as e:
        sys.stdout.write(f"[{datetime.now()}] ERROR: {str(e)}\n")
        raise e

if __name__ == "__main__":
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    INPUT = os.path.join(base_path, "data/silver/breweries")
    OUTPUT = os.path.join(base_path, "data/gold/brewery_metrics")
    
    transform_silver_to_gold(INPUT, OUTPUT)
