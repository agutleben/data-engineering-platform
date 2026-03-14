# dags/dbt_transform_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import os
import logging

PROJECT = os.getenv("GCP_PROJECT_ID", "ton-projet-gcp")
DBT_DIR = "/opt/airflow/dbt_project"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": 30,
}

def check_raw_data(**context):
    """Vérifie que la table RAW a des données avant de lancer dbt."""
    client = bigquery.Client(project=PROJECT)
    query = f"""
        SELECT COUNT(*) as total
        FROM `{PROJECT}.raw.events`
        WHERE event_date = CURRENT_DATE() - 1
    """
    row = list(client.query(query).result())[0]
    if row.total == 0:
        raise ValueError("Aucune donnée RAW pour hier — ingestion manquante ?")
    logging.info(f"{row.total:,} lignes RAW disponibles ✓")


with DAG(
    dag_id="dbt_transform",
    default_args=default_args,
    description="Transformations dbt : Staging → Marts",
    schedule_interval="0 7 * * *",   # après l'ingestion à 6h
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "transform", "staging", "marts"],
) as dag:

    t1 = PythonOperator(
        task_id="check_raw_data",
        python_callable=check_raw_data,
    )

    t2 = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps",
    )

    t3 = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging",
    )

    t4 = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_DIR} && dbt test --select staging",
    )

    t5 = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts",
    )

    t6 = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_DIR} && dbt test --select marts",
    )

    t7 = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=f"cd {DBT_DIR} && dbt docs generate",
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7