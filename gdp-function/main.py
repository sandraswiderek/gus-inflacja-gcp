import os
import json
import requests
from google.cloud import storage, bigquery

# === Konfiguracja ===
BUCKET_NAME = "gus-bdl"                           # opcjonalnie: zrzut JSON do GCS
DATASET_ID = os.environ.get("DATASET_ID", "bdl_dataset")
TABLE_GDP  = "gdp"                                 # bdl_dataset.gdp (date DATE, value FLOAT64, metric STRING)

def _quarter_end_date(year: int, q: str) -> str:
    """Zwraca datę końca kwartału w formacie YYYY-MM-DD dla 'Q1'..'Q4'."""
    q = q.upper().replace("-", "")
    if q == "Q1":
        return f"{year}-03-31"
    if q == "Q2":
        return f"{year}-06-30"
    if q == "Q3":
        return f"{year}-09-30"
    # domyślnie Q4
    return f"{year}-12-31"

# === Pobranie PKB z Eurostatu (PL, CP_MEUR, B1GQ) ===
def fetch_gdp_rows_for_bq():
    """
    Zwraca listę wierszy zgodnych ze schematem tabeli:
    [{'date': 'YYYY-MM-DD', 'value': float, 'metric': 'gdp'}, ...]
    """
    url = (
        "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
        "namq_10_gdp?geo=PL&unit=CP_MEUR&na_item=B1GQ"
    )
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    data = r.json()

    # Mapowanie indeksu -> etykieta czasu (np. '2024-Q1' lub '2024Q1')
    idx_to_time = {v: k for k, v in data["dimension"]["time"]["category"]["index"].items()}

    out = []
    for idx_str, val in data.get("value", {}).items():
        idx = int(idx_str)
        period = idx_to_time.get(idx)  # np. "2024-Q1" albo "2024Q1"
        if not period:
            continue
        year = int(period[:4])
        raw_q = period[4:]
        # normalizuj kwartal na 'Q1'..'Q4'
        quarter = raw_q.replace("-", "").upper()  # "Q1"
        dt = _quarter_end_date(year, quarter)
        out.append({"date": dt, "value": float(val), "metric": "gdp"})
    return out

# === Zapis pomocniczy do GCS (opcjonalny) ===
def save_to_gcs(data, filename):
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(filename)
        payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in data)
        blob.upload_from_string(payload, content_type="application/json")
        print(f"Saved to GCS: gs://{BUCKET_NAME}/{filename}")
    except Exception as e:
        # Nie blokuj wykonania jeśli GCS niedostępny
        print(f"GCS save warning: {e}")

# === Zapis do BigQuery ===
def save_to_bigquery(rows, table_name):
    if not rows:
        print("Brak danych do zapisu.")
        return
    client = bigquery.Client()
    table_ref = client.dataset(DATASET_ID).table(table_name)
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"Insert to {DATASET_ID}.{table_name} failed: {errors}")
    print(f"Inserted {len(rows)} rows into {DATASET_ID}.{table_name}")

# === Entry point Cloud Function (HTTP) ===
def gdp_fetcher(request):
    rows = fetch_gdp_rows_for_bq()  # -> [{'date','value','metric'}]
    save_to_gcs(rows, "gdp_pol.json")  # opcjonalnie
    save_to_bigquery(rows, TABLE_GDP)
    return (f"Pobrano i zapisano {len(rows)} rekordów PKB do "
            f"{DATASET_ID}.{TABLE_GDP}."), 200

