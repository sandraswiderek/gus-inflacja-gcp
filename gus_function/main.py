import requests
import json
from datetime import datetime
from google.cloud import storage, bigquery

# ------------------------
# KONFIGURACJA
# ------------------------
BUCKET_NAME = "gus-bdl"
DATASET_ID = "bdl_dataset"

# Tabele w BigQuery
TABLE_FUEL = "fuel"
TABLE_CHLEB = "chleb"
TABLE_BEZROBOCIE = "bezrobocie"
TABLE_CPI = "inflacja"
TABLE_ENERGIA = "energia"
TABLE_WYNAGRODZENIE = "wynagrodzenia"

# ID zmiennych z BDL
VARIABLE_ID_FUEL = 196398
VARIABLE_ID_CHLEB = 8260
VARIABLE_ID_BEZROBOCIE = 60270
VARIABLE_ID_CPI = 217230
VARIABLE_ID_ENERGIA = 5071
VARIABLE_ID_WYNAGRODZENIE = 64428


# ------------------------
# FUNKCJE POMOCNICZE
# ------------------------
def fetch_bdl_full(variable_id: int):
    """Pobiera wszystkie dane dla danej zmiennej z API BDL (bez podziału na poziomy)."""
    url = f"https://bdl.stat.gov.pl/api/v1/data/by-variable/{variable_id}?format=json&page-size=100"

    combined_results = []
    page = 0
    while True:
        r = requests.get(f"{url}&page={page}")
        r.raise_for_status()
        data = r.json()
        if "results" in data and data["results"]:
            combined_results.extend(data["results"])
        else:
            break
        if "links" in data and "next" in data["links"]:
            page += 1
        else:
            break
    return combined_results


def save_to_gcs(data, filename):
    """Zapisuje dane do GCS, nadpisując plik jeśli istnieje."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)

    ndjson_data = "\n".join(json.dumps(record, ensure_ascii=False) for record in data)
    blob.upload_from_string(ndjson_data, content_type="application/json")

    print(f"Plik zapisany w GCS: gs://{BUCKET_NAME}/{filename}")


def save_to_bigquery(data, table_name):
    client = bigquery.Client(project="zmiana-cen-i-inflacja-w-polsce")
    table_ref = client.dataset(DATASET_ID).table(table_name)

    try:
        table = client.get_table(table_ref)
        print(f"Znalazłem tabelę {table.full_table_id}, schema: {[f.name+':'+f.field_type for f in table.schema]}")
    except Exception as e:
        print(f"Tabela {table_name} nie istnieje lub problem z dostępem: {e}")
        return

    rows_to_insert = []
    for item in data:
        region = item.get("id")
        region_name = item.get("name")
        for val in item.get("values", []):
            rows_to_insert.append({
                "region_id": region,
                "region_name": region_name,
                "year": int(val.get("year")),
                "month": val.get("month"),
                "value": float(val.get("val")) if val.get("val") not in (None, "", "null") else None
            })

    if not rows_to_insert:
        print(f"Brak danych do wstawienia w {table_name}")
        return

    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"Błędy podczas zapisu do BigQuery w tabeli {table_name}: {errors}")
    else:
        print(f"{len(rows_to_insert)} wierszy dodano do {table_name}.")





# ------------------------
# FUNKCJA GŁÓWNA
# ------------------------
def bdl_data_fetcher(request):
    wskazniki = [
        ("benzyna", VARIABLE_ID_FUEL, TABLE_FUEL),
        ("chleb", VARIABLE_ID_CHLEB, TABLE_CHLEB),
        ("bezrobocie", VARIABLE_ID_BEZROBOCIE, TABLE_BEZROBOCIE),
        ("cpi", VARIABLE_ID_CPI, TABLE_CPI),
        ("energia", VARIABLE_ID_ENERGIA, TABLE_ENERGIA),
        ("wynagrodzenia", VARIABLE_ID_WYNAGRODZENIE, TABLE_WYNAGRODZENIE),
    ]

    results_summary = []

    for name, var_id, table in wskazniki:
        try:
            all_results = fetch_bdl_full(var_id)
            save_to_gcs(all_results, f"{name}.json")  # jeden plik, zawsze nadpisywany
            save_to_bigquery(all_results, table)
            results_summary.append(f"{name}: zapisano dane ({len(all_results)} rekordów)")
        except Exception as e:
            results_summary.append(f"{name}: Błąd - {str(e)}")

    return "\n".join(results_summary), 200




