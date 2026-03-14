# Data Engineering Platform

Pipeline de données e-commerce production-ready.

## Stack
Python · Airflow · BigQuery · dbt · FastAPI · Docker · GitHub Actions · Kafka (optionnel)

## Architecture
Event Generator → Airflow → BigQuery RAW → dbt → BigQuery Marts → FastAPI

## Démarrage rapide

# 1. Cloner et configurer
cp .env.example .env
# Remplir GCP_PROJECT_ID dans .env

# 2. Lancer la stack
docker compose up airflow-init
docker compose up -d

# 3. Générer les données
cd data_generator && python generate_events.py

# 4. Créer les datasets BigQuery
python infra/bigquery_setup.py

# 5. Déclencher les DAGs dans Airflow UI
# http://localhost:8080

## Services
| Service   | URL                        |
|-----------|----------------------------|
| Airflow   | http://localhost:8080      |
| API       | http://localhost:8000      |
| Swagger   | http://localhost:8000/docs |