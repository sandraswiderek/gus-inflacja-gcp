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


def gdp_fetcher(request):
    results = fetch_gdp()
    # zwróć tylko pierwsze 5 rekordów do przeglądarki
    return json.dumps(results[:5], ensure_ascii=False), 200

