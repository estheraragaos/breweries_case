import os
import pytest
import json
from unittest.mock import patch, Mock
from dags.brewery_pipeline import extract_bronze

def test_extract_bronze_api_call():
    """
    Tests the Bronze layer ingestion: fetching data from the API 
    and persisting it as a raw JSON file using Mocks.
    """
    mock_response_data = [{"id": "test-1", "name": "Beer House", "brewery_type": "micro"}]
    
    test_bronze_path = "data/bronze/breweries_raw.json"
    
    with patch('requests.get') as mock_get, \
         patch('dags.brewery_pipeline.BRONZE_FILE', test_bronze_path):
        
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_data
        
        extract_bronze()
        
        assert mock_get.called
        assert mock_get.return_value.status_code == 200
        assert os.path.exists(test_bronze_path)
        
        with open(test_bronze_path, "r", encoding="utf-8") as f:
            saved_data = json.load(f)
        assert saved_data == mock_response_data
