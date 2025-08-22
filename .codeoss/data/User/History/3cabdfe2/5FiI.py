import requests
import json
from datetime import datetime
from google.cloud import storage, bigquery

BUCKET_NAME = "gus-bdl"  
DATASET_ID = "kursy_dataset"
TABLE_NAME = "kursy"

def fetch_fx_history(start_date="2010-01-01", end_date=None):
    if not end_date:
        end_date = datetime.today().strftime("%Y-%m-%d")
    currencies = ["EUR", "USD"]
    results = []
    for cur in currencies:
        url = f"http://api.nbp.pl/api/exchangerates/rates/A/{cur}/{start_date}/{end_date}/?format=json"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        for rate in data["rates"]:
            results.append({
                "date": rate["effectiveDate"],
                "currency": cur,
                "rate": float(rate["mid"])
            })
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
        # Pierwszy import – cała historia od 2010
        print("Brak danych w BigQuery → pobieram pełną historię od 2010...")
        results = fetch_fx_history("2010-01-01")
    else:
        # Kolejne wywołania – pobierz od ostatniej daty + 1 dzień
        start_date = (last_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Pobieram dane od {start_date} do dziś...")
        results = fetch_fx_history(start_date)
    
    if not results:
        return "Brak nowych danych do pobrania.", 200

    save_to_gcs(results)  
    save_to_bigquery(results, TABLE_NAME)

    return f"Pobrano i zapisano {len(results)} rekordów.", 200
