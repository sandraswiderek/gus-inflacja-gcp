gsutil -m cp "gs://${BUCKET_US}/${FILE_NAME}" "gs://${BUCKET_EC2}/${FILE_NAME}"
# ====== 4) GCS (ec2) -> BigQuery tabela w europe-central2 ======
bq --location=${LOCATION_EC2} load   --source_format=PARQUET   --autodetect   "${PROJECT_ID}:${DST_DATASET}.${DST_TABLE}"   "gs://${BUCKET_EC2}/${FILE_NAME}"
# (opcjonalnie) sprawdź, że tabela jest
bq --location=${LOCATION_EC2} ls -n 100 "${PROJECT_ID}:${DST_DATASET}"
# ====== 5) Zbuduj tabelę kwartalną: gdp_fx_quarterly_mt ======
bq --location=${LOCATION_EC2} query --use_legacy_sql=false '
CREATE OR REPLACE TABLE `'"${PROJECT_ID}"'.'"${DST_DATASET}"'.gdp_fx_quarterly_mt`
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2010, 2100)) AS
WITH fx AS (
  SELECT
    EXTRACT(YEAR FROM quarter) AS year,
    CONCAT("Q", CAST(EXTRACT(QUARTER FROM quarter) AS STRING)) AS quarter,
    UPPER(currency) AS currency,
    avg_rate
  FROM `'"${PROJECT_ID}"'.'"${DST_DATASET}"'.'"${DST_TABLE}"'`
  WHERE UPPER(currency) IN ("EUR","USD")
),
fx_pivot AS (
  SELECT
    year, quarter,
    MAX(IF(currency="EUR", avg_rate, NULL)) AS value_eur,
    MAX(IF(currency="USD", avg_rate, NULL)) AS value_usd
  FROM fx
  GROUP BY year, quarter
),
gdp AS (
  SELECT
    year,
    quarter,
    value_mlrd AS value_gdp
  FROM `'"${PROJECT_ID}"'.gdp_dataset.gdp_mlrd`
  WHERE year >= 2010
)
SELECT
  gdp.year, gdp.quarter, gdp.value_gdp,
  fx_pivot.value_eur, fx_pivot.value_usd
FROM gdp
LEFT JOIN fx_pivot USING (year, quarter);
'
# ====== 6) Widok: long + QoQ/YoY ======
bq --location=${LOCATION_EC2} query --use_legacy_sql=false '
CREATE OR REPLACE VIEW `'"${PROJECT_ID}"'.gdp_dataset.gdp_fx_long_quarterly` AS
WITH base AS (
  SELECT
    year,
    CAST(SUBSTR(quarter,2) AS INT64) AS q,
    value_gdp, value_eur, value_usd
  FROM `'"${PROJECT_ID}"'.gdp_dataset.gdp_fx_quarterly_mt`
),
u AS (
  SELECT
    year, q,
    DATE_ADD(DATE(year,1,1), INTERVAL (q-1)*3 MONTH) AS quarter_date,
    REPLACE(metric,"value_","") AS metric,
    value
  FROM base
  UNPIVOT(value FOR metric IN (value_gdp, value_eur, value_usd))
)
SELECT
  year, q, quarter_date, metric, value,
  LAG(value)    OVER (PARTITION BY metric ORDER BY year, q)   AS prev_q,
  LAG(value, 4) OVER (PARTITION BY metric ORDER BY year, q)   AS prev_y,
  ROUND(SAFE_DIVIDE(value - LAG(value)    OVER (PARTITION BY metric ORDER BY year, q),
                    LAG(value)            OVER (PARTITION BY metric ORDER BY year, q)) * 100, 1) AS pct_qoq,
  ROUND(SAFE_DIVIDE(value - LAG(value, 4) OVER (PARTITION BY metric ORDER BY year, q),
                    LAG(value, 4)         OVER (PARTITION BY metric ORDER BY year, q)) * 100, 1) AS pct_yoy
FROM u;
'
echo "✅ Gotowe. Tabela gdp_fx_quarterly_mt i widok gdp_fx_long_quarterly utworzone w ${DST_DATASET} (${LOCATION_EC2})."
# ====== ZMIENNE ======
PROJECT_ID="zmiana-cen-i-inflacja-w-polsce"
LOCATION_EC2="europe-central2"
DST_DATASET="gdp_dataset"
DST_TABLE="gdp_fx_quarterly_mt"
DST_TABLE_EC2="kursy_kwartalne_ec2"
# ====== 1) Tworzymy plik z zapytaniem SQL ======
cat > refresh_gdp_fx_quarterly.sql <<'EOF'
CREATE OR REPLACE TABLE `zmiana-cen-i-inflacja-w-polsce.gdp_dataset.gdp_fx_quarterly_mt`
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2010, 2100)) AS
WITH fx AS (
  SELECT
    EXTRACT(YEAR FROM quarter) AS year,
    CONCAT("Q", CAST(EXTRACT(QUARTER FROM quarter) AS STRING)) AS quarter,
    UPPER(currency) AS currency,
    avg_rate
  FROM `zmiana-cen-i-inflacja-w-polsce.gdp_dataset.kursy_kwartalne_ec2`
  WHERE UPPER(currency) IN ("EUR","USD")
),
fx_pivot AS (
  SELECT
    year, quarter,
    MAX(IF(currency="EUR", avg_rate, NULL)) AS value_eur,
    MAX(IF(currency="USD", avg_rate, NULL)) AS value_usd
  FROM fx
  GROUP BY year, quarter
),
gdp AS (
  SELECT
    year,
    quarter,
    value_mlrd AS value_gdp
  FROM `zmiana-cen-i-inflacja-w-polsce.gdp_dataset.gdp_mlrd`
  WHERE year >= 2010
)
SELECT
  gdp.year, gdp.quarter, gdp.value_gdp,
  fx_pivot.value_eur, fx_pivot.value_usd
FROM gdp
LEFT JOIN fx_pivot USING (year, quarter);
EOF

# ====== 2) Tworzymy Scheduled Query w BigQuery ======
bq query   --location="${LOCATION_EC2}"   --use_legacy_sql=false   --display_name="Refresh GDP & FX Quarterly MT"   --schedule="every day 03:00"   --destination_table="${DST_DATASET}.${DST_TABLE}"   --replace=true   < refresh_gdp_fx_quarterly.sql
bq --location=europe-central2 mk --table PROJECT_ID:gdp_dataset.kursy_kwartalne schema.json
bq --location=europe-central2 mk --table PROJECT_ID:gdp_dataset.kursy_latest schema.json
bq --location=europe-central2 mk --table PROJECT_ID:gdp_dataset.kursy schema.json
bq --location=europe-central2 mk --table zmiana-cen-i-inflacja-w-polsce:gdp_dataset.kursy_kwartalne schema.json bq --location=europe-central2 mk --table zmiana-cen-i-inflacja-w-polsce:gdp_dataset.kursy_kwartalne schema.json
bq --location=europe-central2 mk --table zmiana-cen-i-inflacja-w-polsce:gdp_dataset.kursy_latest schema.json
bq --location=europe-central2 mk   --table zmiana-cen-i-inflacja-w-polsce:gdp_dataset.kursy_kwartalne   kursy_dataset.kursy_kwartalne
gcloud functions deploy gdp_function   --runtime python310   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --region europe-central2   --source ./gdp_function
# GDP
bq --location=europe-central2 mk   --table zmiana-cen-i-inflacja-w-polsce:bdl_dataset.gdp   date:DATE,value:FLOAT,metric:STRING
# Kursy dzienne
bq --location=europe-central2 mk   --table zmiana-cen-i-inflacja-w-polsce:bdl_dataset.kursy   date:DATE,currency:STRING,rate:FLOAT
# Kursy kwartalne
bq --location=europe-central2 mk   --table zmiana-cen-i-inflacja-w-polsce:bdl_dataset.kursy_kwartalne   quarter_date:DATE,currency:STRING,rate:FLOAT
# GDP
gcloud functions deploy gdp-function   --gen2   --runtime python310   --region europe-central2   --source ./gdp_function   --entry-point gdp_function   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --timeout=540s
# KURSY
gcloud functions deploy kursy-function   --gen2   --runtime python310   --region europe-central2   --source ./kursy_function   --entry-point kursy_function   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --timeout=540s
# GDP (masz funkcję gdp_fetcher)
gcloud functions deploy gdp-fetcher   --gen2   --runtime python310   --region europe-central2   --source ./gdp_function   --entry-point gdp_fetcher   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --timeout=540s
# zobacz co masz w regionie
gcloud run services list --region=europe-central2
# jeśli na liście jest "gdp-fetcher", skasuj go:
gcloud run services delete gdp-fetcher --region=europe-central2 --quiet
# GDP
gcloud functions deploy gdp-fetcher   --gen2   --runtime python310   --region europe-central2   --source ./gdp_function   --entry-point gdp_fetcher   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --timeout=540s
# KURSY (jak już będziesz gotowa)
gcloud functions deploy kursy-function   --gen2   --runtime python310   --region europe-central2   --source ./kursy_function   --entry-point kursy_function   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --timeout=540s
gcloud functions deploy gdp-fetcher-v2   --gen2   --runtime python310   --region europe-central2   --source ./gdp_function   --entry-point gdp_fetcher   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --timeout=540s
gcloud functions deploy kursy-function-v2   --gen2   --runtime python310   --region europe-central2   --source ./kursy_function   --entry-point kursy_function   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --timeout=540s
# Pobierz URL-e funkcji
gcloud functions describe gdp-fetcher-v2 --gen2 --region=europe-central2 --format='value(serviceConfig.uri)'
gcloud functions describe kursy-function-v2 --gen2 --region=europe-central2 --format='value(serviceConfig.uri)'
# Wywołaj (zrób backfill, jeśli tabele puste)
curl -m 600 -X POST "<URL_z_gdp-fetcher-v2>"
curl -m 600 -X POST "<URL_z_kursy-function-v2>"
# 1) Pobierz URL-e do zmiennych
GDP_URL=$(gcloud functions describe gdp-fetcher-v2 --gen2 --region=europe-central2 --format='value(serviceConfig.uri)')
FX_URL=$(gcloud functions describe kursy-function-v2 --gen2 --region=europe-central2 --format='value(serviceConfig.uri)')
echo "GDP_URL: $GDP_URL"
echo "FX_URL:  $FX_URL"
# 2) Wywołaj funkcje (backfill, jeśli pusto). Najpierw bez autoryzacji:
curl -m 600 -X POST "$GDP_URL"
curl -m 600 -X POST "$FX_URL"
# 3) Jeśli dostaniesz 403 (brak uprawnień), wywołaj z tokenem:
curl -m 600 -X POST -H "Authorization: Bearer $(gcloud auth print-identity-token)" "$GDP_URL"
curl -m 600 -X POST -H "Authorization: Bearer $(gcloud auth print-identity-token)" "$FX_URL"
# URL funkcji
GDP_URL=$(gcloud functions describe gdp-fetcher-v2 --gen2 --region=europe-central2 --format='value(serviceConfig.uri)')
echo $GDP_URL
# wywołanie (z tokenem na wszelki wypadek)
curl -m 600 -X POST -H "Authorization: Bearer $(gcloud auth print-identity-token)" "$GDP_URL"
SA_GDP=$(gcloud functions describe gdp-fetcher-v2 --gen2 --region=europe-central2 --format='value(serviceConfig.serviceAccountEmail)')
bq --location=europe-central2 update --dataset   --add_iam_member=member=serviceAccount:$SA_GDP,role=roles/bigquery.dataEditor   zmiana-cen-i-inflacja-w-polsce:bdl_dataset
# pobierz e-mail SA funkcji
SA_GDP=$(gcloud functions describe gdp-fetcher-v2 --gen2 --region=europe-central2 --format='value(serviceConfig.serviceAccountEmail)')
echo $SA_GDP
# dodaj uprawnienia do datasetu (użyj add-iam-policy-binding, nie "update --dataset")
bq --location=europe-central2 add-iam-policy-binding   zmiana-cen-i-inflacja-w-polsce:bdl_dataset   --member=serviceAccount:$SA_GDP   --role=roles/bigquery.dataEditor
GDP_URL=$(gcloud functions describe gdp-fetcher-v2 --gen2 --region=europe-central2 --format='value(serviceConfig.uri)')
curl -m 600 -X POST -H "Authorization: Bearer $(gcloud auth print-identity-token)" "$GDP_URL"
# === USTAW ZMIENNE ===
export PROJECT_ID="zmiana-cen-i-inflacja-w-polsce"
export REGION="europe-central2"
export DATASET="bdl_dataset"
# (1) URL funkcji kursów (Gen2) -> zamień nazwę jeśli masz inną niż kursy-function-v2
export FX_URL=$(gcloud functions describe kursy-function-v2 --gen2 --region="$REGION" --format='value(serviceConfig.uri)')
echo "FX_URL=$FX_URL"
gcloud scheduler jobs create http kursy-daily-6   --location="$REGION"   --schedule="0 6 * * *"   --time-zone="Europe/Warsaw"   --http-method=POST   --uri="$FX_URL"
bq --location="$REGION" mk --transfer_config   --display_name="kursy_latest_daily"   --data_source=scheduled_query   --target_dataset="$DATASET"   --schedule="every day 06:02"   --params='{
    "query": "CREATE OR REPLACE TABLE `'"$PROJECT_ID"'.'"$DATASET"'.kursy_latest` AS
              SELECT t.* FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY date DESC) rn
                FROM `'"$PROJECT_ID"'.'"$DATASET"'.kursy`
              ) t WHERE rn = 1;",
    "use_legacy_sql": false,
    "write_disposition": "WRITE_TRUNCATE",
    "priority": "BATCH"
  }'
bq --location="$REGION" mk --transfer_config   --display_name="kursy_kwartalne_monthly"   --data_source=scheduled_query   --target_dataset="$DATASET"   --schedule="1 of month 06:10"   --params='{
    "query": "CREATE OR REPLACE TABLE `'"$PROJECT_ID"'.'"$DATASET"'.kursy_kwartalne` AS
              SELECT DATE_TRUNC(date, QUARTER) AS quarter_date, currency, AVG(rate) AS value
              FROM `'"$PROJECT_ID"'.'"$DATASET"'.kursy`
              GROUP BY quarter_date, currency
              ORDER BY quarter_date, currency;",
    "use_legacy_sql": false,
    "write_disposition": "WRITE_TRUNCATE",
    "priority": "BATCH"
  }'
export PROJECT_ID="zmiana-cen-i-inflacja-w-polsce"
export REGION="europe-central2"
export DATASET="bdl_dataset"
bq --location="$REGION" mk --transfer_config   --display_name="kursy_latest_daily"   --data_source=scheduled_query   --target_dataset="$DATASET"   --schedule="every day 06:00 Europe/Warsaw"   --params='{"query":"CREATE OR REPLACE TABLE `'"$PROJECT_ID"'.'"$DATASET"'.kursy_latest` AS SELECT t.* FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY date DESC) rn FROM `'"$PROJECT_ID"'.'"$DATASET"'.kursy`) t WHERE rn = 1;","use_legacy_sql":false}'
bq --location="$REGION" mk --transfer_config   --display_name="kursy_kwartalne_monthly"   --data_source=scheduled_query   --target_dataset="$DATASET"   --schedule="1 of month 06:00 Europe/Warsaw"   --params='{"query":"CREATE OR REPLACE TABLE `'"$PROJECT_ID"'.'"$DATASET"'.kursy_kwartalne` AS SELECT DATE_TRUNC(date, QUARTER) AS quarter_date, currency, AVG(rate) AS value FROM `'"$PROJECT_ID"'.'"$DATASET"'.kursy` GROUP BY quarter_date, currency ORDER BY quarter_date, currency;","use_legacy_sql":false}'
export PROJECT_ID="zmiana-cen-i-inflacja-w-polsce"
export REGION="europe-central2"
export DATASET="bdl_dataset"
bq --location="$REGION" mk --transfer_config   --display_name="kursy_latest_daily"   --data_source=scheduled_query   --target_dataset="$DATASET"   --schedule="0 6 * * *"   --params='{
    "query": "CREATE OR REPLACE TABLE `'"$PROJECT_ID"'.'"$DATASET"'.kursy_latest` AS \
SELECT t.* FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY date DESC) rn \
FROM `'"$PROJECT_ID"'.'"$DATASET"'.kursy`) t WHERE rn = 1;",
    "use_legacy_sql": false,
    "destination_timezone": "Europe/Warsaw",
    "write_disposition": "WRITE_TRUNCATE",
    "priority": "BATCH"
  }'
# --- USTAW ZMIENNE (Twoje wartości) ---
export PROJECT_ID="zmiana-cen-i-inflacja-w-polsce"
export REGION="europe-central2"
export DATASET="bdl_dataset"
cat > latest_params.json <<'JSON'
{
  "query": "CREATE OR REPLACE TABLE `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy_latest` AS
SELECT t.* FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY currency ORDER BY date DESC) rn
  FROM `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy`
) t
WHERE rn = 1;",
  "use_legacy_sql": false,
  "destination_timezone": "Europe/Warsaw",
  "write_disposition": "WRITE_TRUNCATE",
  "priority": "BATCH"
}
JSON

bq --location="$REGION" mk --transfer_config   --display_name="kursy_latest_daily"   --data_source=scheduled_query   --target_dataset="$DATASET"   --schedule="0 6 * * *"   --params="$(cat latest_params.json)"
# USTAW ZMIENNE
export PROJECT_ID="zmiana-cen-i-inflacja-w-polsce"
export REGION="europe-central2"
export DATASET="bdl_dataset"
# 1) Zapisz zapytanie do pliku (łatwiej poprawnie zacytować)
cat > latest_query.sql <<'SQL'
CREATE OR REPLACE TABLE `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy_latest` AS
SELECT t.*
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY currency ORDER BY date DESC) rn
  FROM `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy`
) t
WHERE rn = 1;
SQL

# 2) Zbuduj JSON params przez jq (eliminuje problem z formatem)
LATEST_PARAMS=$(jq -nc --arg q "$(cat latest_query.sql)" '{
  query: $q,
  use_legacy_sql: false,
  destination_timezone: "Europe/Warsaw",
  write_disposition: "WRITE_TRUNCATE",
  priority: "BATCH"
}')
# 3) Utwórz Scheduled Query
bq --location="$REGION" mk --transfer_config   --display_name="kursy_latest_daily"   --data_source=scheduled_query   --target_dataset="$DATASET"   --schedule="0 6 * * *"   --params="$LATEST_PARAMS"
# 1) Autoryzacja i projekt
gcloud auth login
gcloud config set project zmiana-cen-i-inflacja-w-polsce
gcloud auth list    # (sprawdź, że jedno konto jest ACTIVE)
# 2) Zmienne
export PROJECT_ID="zmiana-cen-i-inflacja-w-polsce"
export REGION="europe-central2"
export DATASET="bdl_dataset"
sandraswiderekk@cloudshell:~ (zmiana-cen-i-inflacja-w-polsce)$ 
# zapytanie do pliku
cat > latest_query.sql <<'SQL'
CREATE OR REPLACE TABLE `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy_latest` AS
SELECT t.*
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY currency ORDER BY date DESC) rn
  FROM `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy`
) t
WHERE rn = 1;
SQL

# JSON params przez jq
LATEST_PARAMS=$(jq -nc --arg q "$(cat latest_query.sql)" '{
  query: $q,
  use_legacy_sql: false,
  destination_timezone: "Europe/Warsaw",
  write_disposition: "WRITE_TRUNCATE",
  priority: "BATCH"
}')
# utworzenie Scheduled Query
bq --location="$REGION" mk --transfer_config   --display_name="kursy_latest_daily"   --data_source=scheduled_query   --target_dataset="$DATASET"   --schedule="0 6 * * *"   --params="$LATEST_PARAMS"
cat > latest_query.sql <<'SQL'
CREATE OR REPLACE TABLE `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy_latest` AS
SELECT t.*
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY currency ORDER BY date DESC) rn
  FROM `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy`
) t
WHERE rn = 1;
SQL

LATEST_PARAMS=$(jq -nc --arg q "$(cat latest_query.sql)" '{
  query: $q,
  use_legacy_sql: false,
  destination_timezone: "Europe/Warsaw",
  write_disposition: "WRITE_TRUNCATE",
  priority: "BATCH"
}')
bq --location="europe-central2" mk --transfer_config   --display_name="kursy_latest_daily"   --data_source=scheduled_query   --target_dataset="bdl_dataset"   --schedule="every day 06:00"   --params="$LATEST_PARAMS"
cat > latest_query.sql <<'SQL'
CREATE OR REPLACE TABLE `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy_latest` AS
SELECT t.*
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY currency ORDER BY date DESC) rn
  FROM `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy`
) t
WHERE rn = 1;
SQL

LATEST_PARAMS=$(jq -nc --arg q "$(cat latest_query.sql)" '{
  query: $q,
  use_legacy_sql: false,
  destination_timezone: "Europe/Warsaw",
  write_disposition: "WRITE_TRUNCATE",
  priority: "BATCH"
}')
bq --location="europe-central2" mk --transfer_config   --display_name="kursy_latest_daily"   --data_source=scheduled_query   --target_dataset="bdl_dataset"   --schedule="every day 06:00"   --params="$LATEST_PARAMS"
gcloud config set project zmiana-cen-i-inflacja-w-polsce
bq --location=europe-central2 mk --dataset zmiana-cen-i-inflacja-w-polsce:bdl_dataset
bq --location=europe-central2 mk   --table zmiana-cen-i-inflacja-w-polsce:bdl_dataset.gdp   year:INT64,quarter:STRING,value:FLOAT64
# w katalogu z main.py i requirements.txt
gcloud functions deploy gdp-fetcher-eu   --gen2   --runtime python310   --region europe-central2   --source .   --entry-point gdp_fetcher   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --timeout=540s
# w katalogu z main.py i requirements.txt
gcloud functions deploy gdp-fetcher-eu   --gen2   --runtime python310   --region europe-central2   --source main.py   --entry-point gdp_fetcher   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --timeout=540s
mkdir ~/gdp-function
cd ~/gdp-function
nano main.py
nano requirements.txt
gcloud functions deploy gdp-fetcher-eu   --gen2   --runtime python310   --region europe-central2   --source .   --entry-point gdp_fetcher   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --timeout=540s
gcloud run services logs read gdp-fetcher-eu --region=europe-central2 --limit=100
nano main.py
bq --location=europe-central2 mk --dataset zmiana-cen-i-inflacja-w-polsce:bdl_dataset || true
bq --location=europe-central2 mk   --table zmiana-cen-i-inflacja-w-polsce:bdl_dataset.gdp   date:DATE,value:FLOAT64,metric:STRING || true
# stojąc w folderze z main.py i requirements.txt
gcloud functions deploy gdp-fetcher-eu   --gen2   --runtime python310   --region europe-central2   --source .   --entry-point gdp_fetcher   --trigger-http   --allow-unauthenticated   --set-env-vars DATASET_ID=bdl_dataset   --timeout=540s
URL=$(gcloud functions describe gdp-fetcher-eu --gen2 --region=europe-central2 --format='value(serviceConfig.uri)')
curl -m 300 -X POST "$URL"
# sprawdź dane:
bq --location=europe-central2 query --use_legacy_sql=false 'SELECT COUNT(*) AS rows FROM `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.gdp`;
 SELECT * FROM `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.gdp`
 ORDER BY date DESC LIMIT 10;'
bq --location=europe-central2 mk   --table zmiana-cen-i-inflacja-w-polsce:bdl_dataset.gdp_mlrd   date:DATE,value:FLOAT64,metric:STRING
# ZMIENNE (podmień bucket jeśli inny)
export PROJECT_ID="zmiana-cen-i-inflacja-w-polsce"
export REGION="europe-central2"
export DATASET="bdl_dataset"
export BUCKET="gus-bdl"
mkdir -p gdp_mlrd_external && cd gdp_mlrd_external
# --- main.py (pobiera z BQ gdp, liczy mld od 2010, zapisuje NDJSON) ---
cat > main.py <<'PY'
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
PY

# --- requirements.txt ---
cat > requirements.txt <<'REQ'
google-cloud-bigquery==3.*
google-cloud-storage==2.*
REQ

# Deploy (Gen2, EU)
gcloud functions deploy gdp-mlrd-fetcher   --gen2 --runtime python310 --region "$REGION"   --source . --entry-point gdp_mlrd_fetcher   --trigger-http --allow-unauthenticated   --set-env-vars DATASET_ID="$DATASET",BUCKET="$BUCKET"   --timeout=180s
export PROJECT_ID="zmiana-cen-i-inflacja-w-polsce"
export REGION="europe-central2"
export DATASET="bdl_dataset"
export BUCKET="gus-bdl"
gcloud functions deploy gdp-mlrd-fetcher   --gen2 --runtime python310 --region "$REGION"   --source . --entry-point gdp_mlrd_fetcher   --trigger-http --allow-unauthenticated   --set-env-vars PROJECT_ID="$PROJECT_ID",DATASET_ID="$DATASET",BUCKET="$BUCKET"   --timeout=180s
SA=$(gcloud functions describe gdp-mlrd-fetcher --gen2 --region "$REGION" \
  --format='value(serviceConfig.serviceAccountEmail)')
# odczyt z datasetu bdl_dataset
bq --location="$REGION" add-iam-policy-binding "$PROJECT_ID:$DATASET"   --member="serviceAccount:$SA" --role="roles/bigquery.dataViewer"
# zapis do bucketa
gsutil iam ch serviceAccount:$SA:objectAdmin gs://$BUCKET
URL=$(gcloud functions describe gdp-mlrd-fetcher --gen2 --region "$REGION" \
  --format='value(serviceConfig.uri)')
curl -s "$URL"
gcloud scheduler jobs create http gdp-mlrd-monthly   --location="$REGION"   --schedule="0 6 1 * *"   --time-zone="Europe/Warsaw"   --http-method=GET   --uri="$URL"
# ==== USTAW ZMIENNE ====
export PROJECT_ID="zmiana-cen-i-inflacja-w-polsce"
export REGION="europe-central2"
export JOB_NAME="kursy-kwartalne-daily"
gcloud config set project "$PROJECT_ID"
# Pobierz URL funkcji (musi być zdeployowana np. jako kursy-kwartalne-fetcher)
URL=$(gcloud functions describe kursy-kwartalne-fetcher \
  --gen2 --region="$REGION" --format='value(serviceConfig.uri)')
echo "FUNCTION URL: $URL"
# Utwórz job (albo zaktualizuj, jeśli istnieje)
if gcloud scheduler jobs describe "$JOB_NAME" --location="$REGION" >/dev/null 2>&1; then   echo "Job istnieje -> aktualizuję...";   gcloud scheduler jobs update http "$JOB_NAME"     --location="$REGION"     --schedule="0 7 * * *"     --time-zone="Europe/Warsaw"     --http-method=GET     --uri="$URL"; else   echo "Tworzę nowy job...";   gcloud scheduler jobs create http "$JOB_NAME"     --location="$REGION"     --schedule="0 7 * * *"     --time-zone="Europe/Warsaw"     --http-method=GET     --uri="$URL"; fi
# Sprawdź joby
gcloud scheduler jobs list --location="$REGION"
# ==== USTAW ZMIENNE ====
export PROJECT_ID="zmiana-cen-i-inflacja-w-polsce"
export REGION="europe-central2"
export DATASET_ID="bdl_dataset"
export JOB_NAME="kursy-kwartalne-daily-07"
gcloud config set project "$PROJECT_ID"
# ==== KATALOG I PLIKI FUNKCJI ====
mkdir -p kursy_kwartalne_cron && cd kursy_kwartalne_cron
cat > main.py <<'PY'
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
PY

cat > requirements.txt <<'REQ'
google-cloud-bigquery==3.*
REQ

# ==== DEPLOY FUNKCJI (Gen2, EU) ====
gcloud functions deploy kursy-kwartalne-fetcher   --gen2 --runtime python310 --region "$REGION"   --source . --entry-point kursy_kwartalne_fetcher   --trigger-http --allow-unauthenticated   --set-env-vars PROJECT_ID="$PROJECT_ID",DATASET_ID="$DATASET_ID"   --timeout=180s
# ==== DAJ FUNKCJI PRAWA DO ZAPISU W DATASECIE ====
SA=$(gcloud functions describe kursy-kwartalne-fetcher --gen2 --region "$REGION" \
  --format='value(serviceConfig.serviceAccountEmail)')
bq --location="$REGION" add-iam-policy-binding "$PROJECT_ID:$DATASET_ID"   --member="serviceAccount:$SA" --role="roles/bigquery.dataEditor"
# ==== POBIERZ URL FUNKCJI ====
URL=$(gcloud functions describe kursy-kwartalne-fetcher --gen2 --region "$REGION" \
  --format='value(serviceConfig.uri)')
echo "FUNCTION URL: $URL"
# ==== UTWÓRZ / ZAKTUALIZUJ JOB CLOUD SCHEDULER 07:00 (Europe/Warsaw) ====
if gcloud scheduler jobs describe "$JOB_NAME" --location="$REGION" >/dev/null 2>&1; then   gcloud scheduler jobs update http "$JOB_NAME"     --location="$REGION"     --schedule="0 7 * * *"     --time-zone="Europe/Warsaw"     --http-method=GET     --uri="$URL"; else   gcloud scheduler jobs create http "$JOB_NAME"     --location="$REGION"     --schedule="0 7 * * *"     --time-zone="Europe/Warsaw"     --http-method=GET     --uri="$URL"; fi
# ==== PODGLĄD JOBÓW W REGIONIE ====
gcloud scheduler jobs list --location="$REGION"
pwd
cd 
ls -la
cd cloudshell_open
ls
cd <twoj_folder_z_projektem>
ls
cd cloudshell_open/SANDRASWIDEREKK
ls
git init
git remote add origin https://github.com/sandraswiderek/gus-inflacja-gcp.git
git add .
git commit -m "Initial project upload"
git branch -M main
git push -u origin main
# 1) Ustaw tożsamość (podaj swój mail z GitHuba)
git config --global user.email "TWÓJ_MAIL_Z_GITHUBA"
git config --global user.name "Sandra Swiderek"   # lub jak chcesz, ważne żeby było
# 2) Sprawdź, że pliki są dodane
git status
# Jeśli potrzeba, dodaj je ponownie
git add .
# 3) Zrób commit (ten krok wcześniej się wywalił)
git commit -m "Initial project upload"
# 4) Upewnij się, że gałąź nazywa się main
git branch -M main
# 5) Wyślij na GitHuba
git push -u origin main
