from google.cloud import bigquery
import os

PROJECT = os.getenv("GCP_PROJECT_ID", "ton-projet-gcp")
bigquery.Client(project=PROJECT, location="EU")

datasets = ["raw", "staging", "marts"]
for ds in datasets:
    dataset = bigquery.Dataset(f"{PROJECT}.{ds}")
    dataset.location = "EU"
    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset '{ds}' créé ✓")