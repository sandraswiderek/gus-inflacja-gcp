import os
import json
import requests
from google.cloud import storage, bigquery

# === Konfiguracja ===
BUCKET_NAME = "gus-bdl"                           # opcjonalnie: gdzie zapisujemy JSON
DATASET_ID = os.environ.get("DATASET_ID", "bdl_dataset")
TABLE_GDP  = "gdp"                                 # tabela: bdl_dataset.gdp (year INT64, quarter STRING, value FLOAT64)

# === Pobranie PKB z Eurostatu (PL, CP_MEUR, B1GQ) ===
def fetch_gdp():
    url = (
        "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
        "namq_10_gdp?geo=PL&unit=CP_MEUR&na_item=B1GQ"
    )
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    data = r.json()

    # Mapowanie indeksu -> etykieta czasu (np. 2024-Q1)
    idx_to_time = {v: k for k, v in data["dimension"]["time"]["category"]["index"].items()}

    out = []
    # data["value"] to słownik: { "0": 123.4, "1": 125.6, ... }
    for idx_str, val in data.get("value", {}).items():
        idx = int(idx_str)
        period = idx_to_time.get(idx)  # np. "2024-Q1" albo "2024Q1" zależnie od serwisu
        if not period:
            continue
        year = int(period[:4])
        # kwartal z etykiety, usuwamy ewentualny myślnik by dostać "Q1"
        quarter = period[4:].replace("-", "") or "Q4"  # awaryjnie
        out.append({"year": year, "quarter": quarter, "value": float(val)})
    return out

# === Zapis pomocniczy do GCS (opcjonalny) ===
def save_to_gcs(data, filename):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)
    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in data)
    blob.upload_from_string(payload, content_type="application/json")
    print(f"Saved to GCS: gs://{BUCKET_NAME}/{filename}")

# === Zapis do BigQuery ===
def save_to_bigquery(rows, table_name):
    if not rows:
        print("Brak danych do zapisu.")
        return
    client = bigquery.Client()
    table_ref = client.dataset(DATASET_ID).table(table_name)
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        # pokaż błąd w logach i przerwij aby było widać w Cloud Run
        raise RuntimeError(f"Insert to {DATASET_ID}.{table_name} failed: {errors}")
    print(f"Inserted {len(rows)} rows into {DATASET_ID}.{table_name}")

# === Entry point Cloud Function (HTTP) ===
def gdp_fetcher(request):
    rows = fetch_gdp()
    # opcjonalnie: zrzut do GCS (możesz usunąć jeśli niepotrzebne)
    try:
        save_to_gcs(rows, "gdp_pol.json")
    except Exception as e:
        print(f"GCS save warning: {e}")
    # zapis do BQ
    save_to_bigquery(rows, TABLE_GDP)
    return (f"Pobrano i zapisano {len(rows)} rekordów PKB do "
            f"{DATASET_ID}.{TABLE_GDP}."), 200
