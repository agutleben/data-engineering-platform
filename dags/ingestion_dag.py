from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from pathlib import Path
import logging
import os

PROJECT     = os.getenv("GCP_PROJECT_ID", "ton-projet-gcp")
DATASET     = os.getenv("GCP_DATASET_RAW", "raw")
TABLE       = f"{PROJECT}.{DATASET}.events"
PARQUET_DIR = Path("/opt/airflow/data_generator/output")

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": 60,
}

def check_files(**context):
    files = list(PARQUET_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"Aucun fichier Parquet trouvé dans {PARQUET_DIR}")
    logging.info(f"{len(files)} fichiers Parquet trouvés")
    context["ti"].xcom_push(key="parquet_files", value=[str(f) for f in files])


def ingest_parquet_to_bq(**context):
    files = context["ti"].xcom_pull(key="parquet_files", task_ids="check_files")
    client = bigquery.Client(project=PROJECT)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,    # ← BigQuery détecte les types depuis le Parquet
    )
    for file_path in files:
        logging.info(f"Chargement de {file_path}...")
        with open(file_path, "rb") as f:
            job = client.load_table_from_file(f, TABLE, job_config=job_config)
            job.result()
        logging.info(f"  → {file_path} chargé ✓")
    total = client.get_table(TABLE).num_rows
    logging.info(f"Ingestion terminée — {total:,} lignes dans {TABLE}")

def validate_ingestion(**context):
    client = bigquery.Client(project=PROJECT)
    query = f"""
        SELECT
            COUNT(*)                   AS total_rows,
            COUNT(DISTINCT event_date) AS nb_dates,
            MIN(event_date)            AS min_date,
            MAX(event_date)            AS max_date,
            COUNTIF(event_id IS NULL)  AS null_event_ids
        FROM `{TABLE}`
    """
    row = list(client.query(query).result())[0]
    logging.info(f"total_rows    : {row.total_rows:,}")
    logging.info(f"nb_dates      : {row.nb_dates}")
    logging.info(f"min_date      : {row.min_date}")
    logging.info(f"max_date      : {row.max_date}")
    logging.info(f"null_event_ids: {row.null_event_ids}")
    if row.null_event_ids > 0:
        raise ValueError(f"{row.null_event_ids} event_ids NULL détectés !")
    if row.total_rows == 0:
        raise ValueError("Table vide après ingestion !")
    logging.info("Validation OK ✓")

with DAG(
    dag_id="ingestion_parquet_to_bigquery",
    default_args=default_args,
    description="Ingestion des fichiers Parquet vers BigQuery RAW",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion", "bigquery", "raw"],
) as dag:

    t1 = PythonOperator(task_id="check_files",                python_callable=check_files)
    t2 = PythonOperator(task_id="ingest_parquet_to_bq",       python_callable=ingest_parquet_to_bq)
    t3 = PythonOperator(task_id="validate_ingestion",         python_callable=validate_ingestion)

    t1 >> t2 >> t3