import requests
import json
from google.cloud import storage, bigquery

BUCKET_NAME = "gus-bdl"
DATASET_ID = "bdl_dataset"
TABLE_GDP = "gdp_pol"

def fetch_gdp():
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/namq_10_gdp?geo=PL&unit=CP_MEUR&na_item=B1GQ"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()

    results = []
    time_map = {v: k for k, v in data["dimension"]["time"]["category"]["index"].items()}

    for idx_str, val in data["value"].items():
        idx = int(idx_str)
        period = time_map.get(idx)
        if period:
            year = int(period[:4])
            quarter = period[4:].replace("-", "")  # usuń minus z klucza
            results.append({
                "year": year,
                "quarter": quarter,
                "value": float(val)
            })

    return results

def save_to_gcs(data, filename):
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

def gdp_fetcher(request):
    results = fetch_gdp()
    save_to_gcs(results, "gdp_pol.json")
    save_to_bigquery(results, TABLE_GDP)
    return f"Pobrano i zapisano {len(results)} rekordów PKB.", 200