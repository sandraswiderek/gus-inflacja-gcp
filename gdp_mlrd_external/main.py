import os, json
from google.cloud import bigquery, storage

DATASET = os.environ.get("DATASET_ID", "bdl_dataset")
BUCKET = os.environ.get("BUCKET", "gus-bdl")
DEST = "gdp_mlrd.json"  # ten sam plik za każdym razem -> nadpisanie

def gdp_mlrd_fetcher(request):
    project = os.environ.get("GCP_PROJECT") or os.environ.get("PROJECT_ID")
    if not project:
        return ("Missing project id", 500)

    src = f"`{project}.{DATASET}.gdp`"
    sql = f"""
      SELECT
        FORMAT_DATE('%Y-%m-%d', date) AS date,
        ROUND(value/1000, 2) AS value,         -- miliony -> miliardy (2 miejsca)
        'gdp_mlrd' AS metric
      FROM {src}
      WHERE EXTRACT(YEAR FROM date) >= 2010
      ORDER BY date
    """

    bq = bigquery.Client()
    rows = list(bq.query(sql).result())

    # NDJSON: jeden rekord = jedna linia
    lines = []
    for r in rows:
        lines.append(json.dumps({
            "date": r["date"],
            "value": float(r["value"]),
            "metric": r["metric"]
        }, ensure_ascii=False))

    storage_client = storage.Client()
    blob = storage_client.bucket(BUCKET).blob(DEST)
    blob.upload_from_string("\n".join(lines), content_type="application/json")
    return (f"Saved gs://{BUCKET}/{DEST} with {len(lines)} rows", 200)
