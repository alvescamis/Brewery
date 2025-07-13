import json
import requests
from google.cloud import storage

def fetch_and_store_json(date):
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.get(url)
    response.raise_for_status()

    breweries = response.json()
    client = storage.Client()
    bucket = client.bucket("hdfs-datalake-brewery")
    blob = bucket.blob(f"bronze/{date}/breweries.json")
    blob.upload_from_string(json.dumps(breweries), content_type='application/json')
