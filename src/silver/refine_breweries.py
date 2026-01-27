import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_spark_session():
    """Spark session with Delta Lake and Java compatibility flags."""
    return SparkSession.builder \
        .appName("BrewerySilverDelta") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED") \
        .getOrCreate()

def transform_bronze_to_silver(input_path, output_path):
    """
    Reads raw data from Bronze, applies cleaning, normalization, 
    and persists it in Delta format partitioned by country.
    """
    spark = get_spark_session()
    sys.stdout.write(f"[{datetime.now()}] Applying data cleansing and normalization for Silver layer...\n")

    try:
        bronze_data = spark.read.json(input_path)

        silver_data = bronze_data \
            .withColumn("id", F.trim(F.col("id"))) \
            .withColumn("latitude", F.col("latitude").cast("double")) \
            .withColumn("longitude", F.col("longitude").cast("double")) \
            .withColumn("brewery_type", F.lower(F.trim(F.col("brewery_type")))) \
            .withColumn("country", F.lower(F.trim(F.col("country")))) \
            .withColumn("city", F.lower(F.trim(F.col("city")))) \
            .withColumn("state_province", F.lower(F.trim(F.col("state_province")))) \
            .withColumn("country", F.coalesce(F.col("country"), F.lit("unknown_country"))) \
            .withColumn("brewery_type", F.coalesce(F.col("brewery_type"), F.lit("not_informed"))) \
            .withColumn("name", F.coalesce(F.col("name"), F.lit("unnamed_brewery"))) \
            .dropDuplicates(["id"]) \
            .withColumn("transformed_at", F.current_timestamp())

        final_columns = [
            "id", "name", "brewery_type", "city", "state_province", 
            "postal_code", "country", "longitude", "latitude", 
            "phone", "website_url", "transformed_at"
        ]
        
        silver_data = silver_data.select(*final_columns)

        silver_data.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("country") \
            .option("overwriteSchema", "true") \
            .save(output_path)

        sys.stdout.write(f"[{datetime.now()}] Silver layer finalized. Total records persisted: {silver_data.count()}\n")

    except Exception as e:
        sys.stdout.write(f"[{datetime.now()}] ERROR: {str(e)}\n")
        raise e

if __name__ == "__main__":
    import os
    
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    
    INPUT = os.path.join(base_path, "data/bronze/breweries")
    OUTPUT = os.path.join(base_path, "data/silver/breweries")
    
    transform_bronze_to_silver(INPUT, OUTPUT)
