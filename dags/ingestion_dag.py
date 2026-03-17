import os
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from pathlib import Path
import logging
from datetime import timedelta

PROJECT         = os.getenv("GCP_PROJECT_ID", "data-pipeline-490114")
DATASET         = os.getenv("GCP_DATASET_RAW", "raw")
TABLE           = f"{PROJECT}.{DATASET}.events"
PARQUET_DIR     = Path("/opt/airflow/data_generator/output")
GENERATE_SCRIPT = "/opt/airflow/data_generator/generate_events.py"

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=60),
}

def generate_events(**context):
    for f in PARQUET_DIR.glob("*.parquet"):
        f.unlink()
    result = subprocess.run(
        ["python", GENERATE_SCRIPT],
        capture_output=True, text=True,
        cwd="/opt/airflow/data_generator"
    )
    if result.returncode != 0:
        raise Exception(f"Génération échouée:\n{result.stderr}")
    logging.info(result.stdout)

def check_files(**context):
    files = list(PARQUET_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"Aucun fichier Parquet trouvé dans {PARQUET_DIR}")
    logging.info(f"{len(files)} fichiers Parquet trouvés")
    context["ti"].xcom_push(key="parquet_files", value=[str(f) for f in files])

def ingest_parquet_to_bq(**context):
    files = context["ti"].xcom_pull(key="parquet_files", task_ids="check_files")
    client = bigquery.Client(project=PROJECT, location="EU")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
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
    client = bigquery.Client(project=PROJECT, location="EU")
    query = f"""
        SELECT
            COUNT(*)                   AS total_rows,
            COUNTIF(event_id IS NULL)  AS null_event_ids
        FROM `{TABLE}`
    """
    row = list(client.query(query).result())[0]
    logging.info(f"total_rows    : {row.total_rows:,}")
    if row.null_event_ids > 0:
        raise ValueError(f"{row.null_event_ids} event_ids NULL détectés !")
    if row.total_rows == 0:
        raise ValueError("Table vide après ingestion !")
    logging.info("Validation OK ✓")

with DAG(
    dag_id="ingestion_pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion", "bigquery"],
) as dag:

    t0 = PythonOperator(task_id="generate_events",    python_callable=generate_events)
    t1 = PythonOperator(task_id="check_files",        python_callable=check_files)
    t2 = PythonOperator(task_id="ingest_to_bq",       python_callable=ingest_parquet_to_bq)
    t3 = PythonOperator(task_id="validate_ingestion", python_callable=validate_ingestion)
    t4 = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dbt_cosmos_pipeline",
        wait_for_completion=True,
    )

    t0 >> t1 >> t2 >> t3 >> t4