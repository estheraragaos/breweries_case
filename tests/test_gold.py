import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from dags.brewery_pipeline import run_gold_transformation

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("GoldIntegrationTest") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED") \
        .getOrCreate()

def test_run_gold_transformation_flow(spark):
    """
    Validates the Gold layer aggregation logic by mocking Silver input 
    with a complete schema and verifying the aggregation output.
    """
    test_input = "data/silver/breweries_delta_test"
    test_output = "data/test_gold_metrics"

    sample_data = [
        ("brewery-1", "micro", "Austin", "Texas", "united states"),
        ("brewery-2", "micro", "Austin", "Texas", "united states"),
        ("brewery-3", "regional", "Dallas", "Texas", "united states")
    ]
    columns = ["id", "brewery_type", "city", "state", "country"]
    
    df_silver = spark.createDataFrame(sample_data, columns)
    
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(test_input)

    with patch('dags.brewery_pipeline.SILVER_OUTPUT', test_input), \
         patch('dags.brewery_pipeline.GOLD_PATH', test_output):
        
        run_gold_transformation()
        
        df_result = spark.read.format("delta").load(test_output)
        micro_count = df_result.filter("brewery_type = 'micro'").select("brewery_count").collect()[0][0]
        
        assert micro_count == 2
        assert df_result.count() == 2
