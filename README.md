# 📊 gus-inflacja-gcp

## Projekt

Dashboard inflacji na podstawie danych GUS BDL i Google Cloud Platform  
**ENG**: *Inflation trends dashboard using Polish GUS BDL API and Google Cloud Platform (BigQuery, Cloud Functions, Looker Studio)*

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

Plik zawiera funkcję do pobierania danych z API GUS i zapisania ich do bucketa.

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
    bucket = client.get_bucket("inflacja-gus-raw-data")
    blob = bucket.blob(filename)

    blob.upload_from_string(
        data=json.dumps(data),
        content_type="application/json"
    )

    return "Dane zapisane do Cloud Storage."
```

---

### 3. Utworzenie pliku requirements.txt
# Zawiera zależności potrzebne do działania funkcji:

```txt requests google-cloud-storage 
```

---

### 4. Wdrożenie funkcji jako Cloud Function
# Funkcja fetch_gus_data została wdrożona do Google Cloud za pomocą:

```gcloud functions deploy fetch_gus_data \
  --runtime python39 \
  --trigger-http \
  --entry-point fetch_gus_data \
  --region europe-central2 \
  --allow-unauthenticated \
  --no-gen2
```

---

### 5. Wynik działania funkcji
# Po wywołaniu funkcji dane są zapisywane jako pliki JSON w buckecie inflacja-gus-raw-data. Przykład pliku:

```gus_inflation_2025-07-23.json```

---

### 6. Załadowanie danych do BigQuery
# Przejście do BigQuery → Utwórz tabelę → Źródło: Cloud Storage

---

### 7. Rozwinięcie danych w SQL
# Aby dostać się do danych takich jak year, month, value, należy rozpakować zagnieżdżone pola JSON przy użyciu UNNEST():

```SELECT
  v.year,
  v.month,
  v.val AS value,
  v.unitName
FROM
  `zmiana-cen-i-inflacja-w-polsce.inflacja_dataset.gus_json_raw`,
  UNNEST(results) AS r,
  UNNEST(r.values) AS v
LIMIT 10;
```
