import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from minio import Minio
import os

# Logger
logger = logging.getLogger(__name__)

# MinIO and PostgreSQL configuration via Airflow Connections
MINIO_CONN_ID = "minio_conn"      
RAW_BUCKET = "raw-data"
PROCESSED_BUCKET = "processed-data"
RAW_FILE = "orders.csv"
PROCESSED_FILE = "orders_transformed.parquet"

POSTGRES_CONN_ID = "postgres_conn"  
TABLE_NAME = "orders"

# -------------------------
# Setup Buckets
# -------------------------
def setup_buckets():
    """Check and create MinIO buckets if they do not exist"""
    logger.info("Checking/creating MinIO buckets")

    # Use Airflow S3Hook to maintain consistency
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    client = hook.get_conn()  # boto3 client under the hood
    
    for bucket in [RAW_BUCKET, PROCESSED_BUCKET]:
        exists = hook.check_for_bucket(bucket_name=bucket)
        if not exists:
            client.create_bucket(Bucket=bucket)
            logger.info(f"Bucket '{bucket}' created in MinIO")
        else:
            logger.info(f"Bucket '{bucket}' already exists")

# -------------------------
# Upload CSV to MinIO
# -------------------------
def upload_to_minio():
    """Upload a local CSV to the raw bucket using MinIO client"""
    logger.info("Uploading CSV to MinIO bucket")
    
    # MinIO credentials from environment (más seguro)
    minio_client = Minio(
        "minio:9000",  # nombre del servicio Docker
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=False
    )
    
    local_file = "/opt/airflow/data/raw/orders.csv"
    if not os.path.exists(local_file):
        raise FileNotFoundError(f"File not found: {local_file}")
    
    minio_client.fput_object(
        bucket_name=RAW_BUCKET,
        object_name=RAW_FILE,
        file_path=local_file
    )
    logger.info(f"File {RAW_FILE} uploaded to bucket {RAW_BUCKET}")

# -------------------------
# ETL Functions
# -------------------------
def extract(**kwargs):
    logger.info("Extracting raw data from MinIO")
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    
    # Download CSV to a temporary local file
    local_file = f"/tmp/{RAW_FILE}"
    hook.get_key(RAW_FILE, bucket_name=RAW_BUCKET).download_file(local_file)
    
    df = pd.read_csv(local_file)
    logger.info(f"Extracted {len(df)} rows from {RAW_FILE}")
    
    # Save temporary file for transformation
    temp_file = f"/tmp/{PROCESSED_FILE}"
    df.to_parquet(temp_file, index=False)
    kwargs['ti'].xcom_push(key='processed_file', value=temp_file)


def transform(**kwargs):
    logger.info("Transforming data")
    temp_file = kwargs['ti'].xcom_pull(key='processed_file', task_ids='extract')
    
    df = pd.read_parquet(temp_file)
    
    # Cleaning and typing
    df = df.dropna(subset=['order_id', 'customer_id', 'product_name', 'price'])
    df['price'] = df['price'].astype(float)
    df['quantity'] = df['quantity'].astype(int)
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    # Save transformed parquet to MinIO
    processed_path = f"/tmp/{PROCESSED_FILE}"
    df.to_parquet(processed_path, index=False)
    
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    hook.load_file(filename=processed_path, key=PROCESSED_FILE, bucket_name=PROCESSED_BUCKET, replace=True)
    
    logger.info(f"Transformed data saved to MinIO bucket {PROCESSED_BUCKET}/{PROCESSED_FILE}")
    kwargs['ti'].xcom_push(key='processed_file', value=processed_path)


def load(**kwargs):
    logger.info("Loading data into PostgreSQL")
    temp_file = kwargs['ti'].xcom_pull(key='processed_file', task_ids='transform')
    
    df = pd.read_parquet(temp_file)
    
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME} (order_id, customer_id, product_name, category, price, quantity, order_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """, (
            row['order_id'],
            row['customer_id'],
            row['product_name'],
            row['category'],
            row['price'],
            row['quantity'],
            row['order_date']
        ))
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"Loaded {len(df)} rows into PostgreSQL")

# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="etl_with_minio",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "ecommerce", "minio", "postgres"],
) as dag:

    task_setup_buckets = PythonOperator(
        task_id="setup_buckets",
        python_callable=setup_buckets,
    )

    task_upload_to_minio = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
    )

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

    task_setup_buckets >> task_upload_to_minio >> task_extract >> task_transform >> task_load
