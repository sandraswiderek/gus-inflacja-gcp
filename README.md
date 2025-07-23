# gus-inflacja-gcp
PL: Projekt: Dashboard inflacji na podstawie danych GUS BDL i Google Cloud Platform
ENG: Inflation trends dashboard using Polish GUS BDL API and Google Cloud Platform (BigQuery, Cloud Functions, Looker Studio)

Cel:
Stworzenie automatycznego pipeline’u danych z Głównego Urzędu Statystycznego (GUS) w celu wizualizacji inflacji i zmian cen w Polsce. Dane są pobierane z API BDL, przetwarzane w GCP i prezentowane w interaktywnym dashboardzie.

Użyte technologie:
- Google Cloud Functions (pobieranie danych z API)
- Google Cloud Storage (przechowywanie JSON)
- BigQuery (hurtownia danych i analiza)
- Looker Studio (dashboard interaktywny)
- Python (żądanie HTTP, przetwarzanie danych)
- GitHub (dokumentacja i wersjonowanie)

Proces:
1. Utworzenie bucketa 'inflacja-gus-raw-data' (lokalizacja: europe-central2, klasa pamięci: autoclass)
2. Dwa pliki:
1) main.py
import requests
import json
import datetime
from google.cloud import storage

def fetch_gus_data(request):
    url = "https://bdl.stat.gov.pl/api/v1/data/by-variable/60495?format=json"
    response = requests.get(url)
    data = response.json()

    today = datetime.date.today().isoformat()
    filename = f"gus_inflation_{today}.json"

    client = storage.Client()
    bucket = client.get_bucket("inflacja-gus-raw-data")  # ← tu nic nie zmieniaj, wszystko OK
    blob = bucket.blob(filename)

    blob.upload_from_string(
        data=json.dumps(data),
        content_type="application/json"
    )

    return "Dane zapisane do Cloud Storage."
2) requirements.txt
requests
google-cloud-storage

Wdrożenie funkcji w cloudshell:
gcloud functions deploy fetch_gus_data \
  --runtime python310 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point fetch_gus_data \
  --region europe-central2

