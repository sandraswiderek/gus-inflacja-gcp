# Price Changes and Inflation in Poland
*Cloud-based data pipeline and dashboard with economic indicators of Poland.*

## Project

Interactive data pipeline and dashboard showing macroeconomic indicators in Poland (inflation, wages, fuel, energy, bread, unemployment, exchange rates).

![Python 3.10](https://img.shields.io/badge/Python-3.10-blue) ![BigQuery](https://img.shields.io/badge/BigQuery-GCP-blueviolet) ![Looker Studio](https://img.shields.io/badge/Looker%20Studio-Data%20Viz-orange)

---

## 🎯 Project Goal

Design and implement a cloud-based data pipeline for collecting, processing, and visualizing macroeconomic indicators of Poland. The dashboard provides insights into price dynamics, inflation, wages, and exchange rates.

---

## 📐 Project Architecture
- Data ingested from public APIs: GUS, Eurostat, and NBP (exchange rates),
- Raw data stored in **Google Cloud Storage**,
- Data processed and loaded into **BigQuery**,
- Automated data refresh using **Cloud Scheduler**,
- Visualization created in **Looker Studio** (interactive dashboard with year selector). 

---

## 📊 Pipeline flow
```[APIs] → [Cloud Storage] → [BigQuery] → [Looker Studio]```

---

## Technologies
- ☁️ **Google Cloud Platform**: BigQuery, Cloud Storage, Cloud Scheduler  
- 🐍 **Python** (requests, pandas, google-cloud-bigquery)  
- 💾 **SQL**  
- 📊 **Looker Studio**  

---

## Features
- 🔄 Automated data ingestion from GUS, Eurostat, and NBP APIs,
- ⏱️ Scheduled updates of BigQuery datasets,
- 🖥️ Interactive dashboard with year selection and comparison charts, 
- 📈 Historical data analysis of inflation, wages, product prices (bread, fuel, energy), unemployment, and exchange rates.

---

## 🚀 Getting Started 

1. Clone the repository

```bash
git clone https://github.com/sandraswiderek/macro-dashboard-pl
cd macro-dashboard-pl
```


2. Install dependencies for each function

*Example for GUS function*

```bash
cd gus_function
pip install -r requirements.txt
```


3. Deploy Cloud Functions

*Example for GUS function*

```bash
gcloud functions deploy gus_function \
  --runtime python310 \
  --trigger-http \
  --allow-unauthenticated
```

(Repeat steps 2 and 3 for `gdp_function` and `kursy_function`)



4. Run SQL transformations in BigQuery

* `latest_query.sql` → loads the most recent macroeconomic data
* `refresh_gdp_fx_quarterly.sql` → updates GDP and exchange rate data quarterly


5. Connect BigQuery to Looker Studio
* build interactive dashboard in Looker Studio connected to BigQuery (year selector + comparison charts)

---

## Dashboard Demo

Live Looker Studio Dashboard: [![Live Dashboard](https://img.shields.io/badge/Live%20Dashboard-Blue)](https://lookerstudio.google.com/reporting/ad47fc1c-771f-4632-bb84-a67096e62b93)

Example screenshot:

<img width="986" height="737" alt="image" src="https://github.com/user-attachments/assets/1a2590f8-410d-48b3-819c-fc4bfdcd7baa" />

---

## Future Improvements

* adding more economic indicators (housing prices, interest rates)
* expanding dashboard interactivity with custom filters

