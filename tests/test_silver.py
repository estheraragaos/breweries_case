import pytest
import json
import os
from unittest.mock import patch
from pyspark.sql import SparkSession
from dags.brewery_pipeline import run_silver_transformation

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("SilverIntegrationTest") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def test_run_silver_transformation_flow(spark):
    """
    Validates Silver transformation by providing a mock JSON that matches 
    the real Open Brewery DB API schema.
    """
    test_input = "data/bronze/breweries_test_full.json"
    test_output = "data/test_silver_output"
    
    mock_real_api_data = [{
        "id": "5128df48-79fc-4f0f-8b52-d05be5492190",
        "name": "  (512) Brewing Co  ",
        "brewery_type": "Micro",
        "address_1": "407 Radam Ln Ste F200",
        "city": "Austin",
        "state_province": "Texas",
        "postal_code": "78745-1197",
        "country": "United States",
        "longitude": "-97.770158",
        "latitude": "30.222076",
        "phone": "5129142467",
        "website_url": "http://www.512brewing.com",
        "state": "Texas",
        "street": "407 Radam Ln Ste F200"
    }]
    
    os.makedirs(os.path.dirname(test_input), exist_ok=True)
    with open(test_input, "w") as f:
        json.dump(mock_real_api_data, f)

    with patch('dags.brewery_pipeline.BRONZE_FILE', test_input), \
         patch('dags.brewery_pipeline.SILVER_OUTPUT', test_output):
        
        try:
            run_silver_transformation()
            success = True
        except Exception as e:
            print(f"Transformation Failed: {e}")
            success = False

    assert success is True