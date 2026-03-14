# dags/ingestion_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery, storage
from pathlib import Path
import pandas as pd
import os
import logging

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT    = os.getenv("GCP_PROJECT_ID", "ton-projet-gcp")
DATASET    = os.getenv("GCP_DATASET_RAW", "raw")
TABLE      = f"{PROJECT}.{DATASET}.events"
PARQUET_DIR = Path("/opt/airflow/dbt_project/../data_generator/output")

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": 60,  # secondes
}

# ── Tasks ─────────────────────────────────────────────────────────────────────
def check_files(**context):
    """Vérifie que des fichiers Parquet sont disponibles."""
    files = list(PARQUET_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"Aucun fichier Parquet trouvé dans {PARQUET_DIR}")
    logging.info(f"{len(files)} fichiers Parquet trouvés")
    context["ti"].xcom_push(key="parquet_files", value=[str(f) for f in files])


def create_table_if_not_exists(**context):
    """Crée la table BigQuery RAW avec partitionnement et clustering."""
    client = bigquery.Client(project=PROJECT)

    schema = [
        bigquery.SchemaField("event_id",   "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("event_type", "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("user_id",    "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("product_id", "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("category",   "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("amount",     "FLOAT64",   mode="NULLABLE"),
        bigquery.SchemaField("event_date", "DATE",      mode="REQUIRED"),
        bigquery.SchemaField("event_ts",   "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("session_id", "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("device",     "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("country",    "STRING",    mode="NULLABLE"),
    ]

    table_ref = bigquery.Table(TABLE, schema=schema)
    table_ref.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="event_date",
    )
    table_ref.clustering_fields = ["user_id", "product_id"]
    client.create_table(table_ref, exists_ok=True)
    logging.info(f"Table {TABLE} prête ✓")