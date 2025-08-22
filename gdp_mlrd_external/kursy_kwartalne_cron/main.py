import os
from google.cloud import bigquery

DATASET_ID = os.environ.get("DATASET_ID", "bdl_dataset")

def kursy_kwartalne_fetcher(request):
    project_id = os.environ.get("PROJECT_ID") or os.environ.get("GCP_PROJECT")
    if not project_id:
        return ("Missing PROJECT_ID", 500)

    src = f"`{project_id}.{DATASET_ID}.kursy`"
    dst = f"`{project_id}.{DATASET_ID}.kursy_kwartalne`"

    sql = f"""
    CREATE OR REPLACE TABLE {dst} AS
    SELECT
      DATE_TRUNC(date, QUARTER) AS quarter_date,
      currency,
      ROUND(AVG(rate), 4) AS value
    FROM {src}
    GROUP BY quarter_date, currency
    ORDER BY quarter_date, currency
    """
    bq = bigquery.Client()
    bq.query(sql).result()
    return ("kursy_kwartalne refreshed", 200)
