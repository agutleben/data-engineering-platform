from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from cosmos import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig

from google.cloud import bigquery
import os
import logging

PROJECT = os.getenv("GCP_PROJECT_ID")

def check_raw_data():
    client = bigquery.Client(project=PROJECT)

    query = f"""
    SELECT COUNT(*) as total
    FROM `{PROJECT}.raw.events`
    WHERE event_date = CURRENT_DATE() - 1
    """

    row = list(client.query(query).result())[0]

    if row.total == 0:
        raise ValueError("Aucune donnée RAW trouvée")

    logging.info(f"{row.total} lignes RAW trouvées ✓")


with DAG(
    dag_id="dbt_cosmos_pipeline",
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=["dbt"],
) as dag:

    check_data = PythonOperator(
        task_id="check_raw_data",
        python_callable=check_raw_data
    )

    dbt_tasks = DbtTaskGroup(
        group_id="dbt",

        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dbt_project"
        ),

        profile_config=ProfileConfig(
            profile_name="data_engineering_platform",
            target_name="dev",
            profiles_yml_filepath="/opt/airflow/dbt_project/profiles.yml"
        ),
    )

    check_data >> dbt_tasks