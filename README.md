# 📊 gus-inflacja-gcp

## 🇵🇱 Projekt

Dashboard inflacji na podstawie danych GUS BDL i Google Cloud Platform  
🇬🇧 *Inflation trends dashboard using Polish GUS BDL API and Google Cloud Platform (BigQuery, Cloud Functions, Looker Studio)*

---

## 🎯 Cel projektu

Stworzenie automatycznego pipeline’u danych z Głównego Urzędu Statystycznego (GUS), przetwarzanych w Google Cloud Platform i prezentowanych w Looker Studio jako interaktywny dashboard do analizy inflacji i zmian cen w Polsce.

---

## 🧰 Użyte technologie

- **Google Cloud Functions** – pobieranie danych z API
- **Google Cloud Storage** – przechowywanie JSON
- **BigQuery** – hurtownia danych i analiza SQL
- **Looker Studio** – interaktywny dashboard
- **Python** – żądania HTTP i przetwarzanie danych
- **GitHub** – dokumentacja i wersjonowanie

---

## 🧱 Proces

### 1. Utworzenie bucketa Cloud Storage
- Nazwa: `inflacja-gus-raw-data`
- Lokalizacja: `europe-central2 (Warszawa)`
- Klasa pamięci: `Autoclass`

---

### 2. Stworzenie pliku `main.py`

```python
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
    bucket = client.get_bucket("inflacja-gus-raw-data")  # <- tu nic nie zmieniaj
    blob = bucket.blob(filename)

    blob.upload_from_string(
        data=json.dumps(data),
        content_type="application/json"
    )

    return "Dane zapisane do Cloud Storage."
