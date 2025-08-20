import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

logger = logging.getLogger(__name__)

def extract():
    logger.info("🔹 Extracting raw data...")

def transform():
    logger.info("🔹 Transforming data...")

def load():
    logger.info("🔹 Loading data into PostgreSQL...")

with DAG(
    dag_id="etl_ecommerce",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "ecommerce"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        retries=2,
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    task_extract >> task_transform >> task_load
