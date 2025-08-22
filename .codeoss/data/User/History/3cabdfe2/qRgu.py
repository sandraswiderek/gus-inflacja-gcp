import requests
import json
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
import os

BUCKET_NAME = "gus-bdl"  
DATASET_ID = os.environ.get("DATASET_ID", "bdl_dataset")
TABLE_NAME = "kursy"

def fetch_fx_history(start_date, end_date=None):
    """Pobiera kursy EUR i USD z NBP w zadanym zakresie dat."""
    if not end_date:
        end_date = datetime.today().strftime("%Y-%m-%d")
    currencies = ["EUR", "USD"]
    results = []
    for cur in currencies:
        url = f"http://api.nbp.pl/api/exchangerates/rates/A/{cur}/{start_date}/{end_date}/?format=json"
        print(f"Pobieram {cur} od {start_date} do {end_date}")
        r = requests.get(url)
        if r.status_code == 404:
            print(f"Brak danych dla {cur} w zakresie {start_date} - {end_date}")
            continue
        r.raise_for_status()
        data = r.json()
        for rate in data["rates"]:
            results.append({
                "date": rate["effectiveDate"],
                "currency": cur,
                "rate": float(rate["mid"])
            })
    return results

def fetch_full_history():
    """Pobiera całą historię od 2010 roku rok po roku."""
    results = []
    current_year = datetime.today().year
    for year in range(2010, current_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31" if year < current_year else datetime.today().strftime("%Y-%m-%d")
        results.extend(fetch_fx_history(start_date, end_date))
    return results

def get_last_date_in_bigquery():
    client = bigquery.Client()
    query = f"""
        SELECT MAX(date) as last_date
        FROM `{client.project}.{DATASET_ID}.{TABLE_NAME}`
    """
    result = client.query(query).result()
    for row in result:
        return row.last_date
    return None

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

def kursy_function(request):
    last_date = get_last_date_in_bigquery()
    
    if last_date is None:
        print("Brak danych w BigQuery → pobieram pełną historię od 2010...")
        results = fetch_full_history()
    else:
        start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        today = datetime.today().strftime("%Y-%m-%d")
        if start_date > today:
            return "Brak nowych danych do pobrania.", 200
        print(f"Pobieram dane od {start_date} do dziś...")
        results = fetch_fx_history(start_date, today)
    
    if not results:
        return "Brak nowych danych do pobrania.", 200

    save_to_gcs(results)  
    save_to_bigquery(results, TABLE_NAME)

    return f"Pobrano i zapisano {len(results)} rekordów.", 200
