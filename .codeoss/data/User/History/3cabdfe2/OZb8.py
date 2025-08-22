import requests
import json
from datetime import datetime
from google.cloud import storage, bigquery

BUCKET_NAME = "gus-bdl"  # Twój bucket GCS
DATASET_ID = "kursy_dataset"
TABLE_NAME = "kursy"

def fetch_fx():
    url = "http://api.nbp.pl/api/exchangerates/tables/A/?format=json"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()[0]
    
    today = data["effectiveDate"]
    results = []
    for item in data["rates"]:
        if item["code"] in ["EUR", "USD"]:
            results.append({
                "date": today,
                "currency": item["code"],
                "rate": float(item["mid"])
            })
    return results

def save_to_gcs(data, filename="kursy_walut.json"):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)
    json_data = "\n".join(json.dumps(record, ensure_ascii=False) for record in data)
    blob.upload_from_string(json_data, content_type="application/json")
    print(f"Plik zapisany w GCS: gs://{BUCKET_NAME}/{filename}")

def save_to_bigquery(data, table_name):
    client = bigquery.Client()
    table_ref = client.dataset(DATASET_ID).table(table_name)
    errors = client.insert_rows_json(table_ref, data)
    if errors:
        print(f"Błędy przy zapisie do {table_name}: {errors}")
    else:
        print(f"{len(data)} rekordów dodano do {table_name}")

def fx_fetcher(request):
    results = fetch_fx()
    save_to_gcs(results)  # zawsze nadpisuje kursy_walut.json
    save_to_bigquery(results, TABLE_NAME)
    return f"Pobrano kursy walut: {len(results)} rekordów", 200
