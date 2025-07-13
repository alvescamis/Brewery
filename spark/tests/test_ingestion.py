import pytest
from unittest.mock import patch, MagicMock
from spark.ingestion import fetch_and_store_json

@patch("spark.ingestion.requests.get")
@patch("spark.ingestion.storage.Client")
def test_fetch_and_store_json(mock_client, mock_get):
    mock_get.return_value.json.return_value = [{"id":1,"name":"Test"}]
    mock_get.return_value.raise_for_status.return_value = None

    mock_blob = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client.return_value.bucket.return_value = mock_bucket

    fetch_and_store_json("2025-07-12")

    mock_blob.upload_from_string.assert_called_once()
