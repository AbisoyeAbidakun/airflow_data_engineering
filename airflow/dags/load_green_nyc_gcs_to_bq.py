import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
    BigQueryCreateExternalTableOperator,
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
SERVICE = "green"
SOURCE_OBJECT = SERVICE +'_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv' #green_tripdata_2020-01.csv
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
SOURCE_BUCKET = os.environ.get("GCP_GCS_BUCKET")
DATASET="nyc"
DATETIME_COLUMN = "lpep_pickup_datetime"
FILE_FORMAT= "CSV"

BQ_QUERY = f"""CREATE TABLE IF NOT EXISTS `{DATASET}.{SERVICE}_{DATASET}_data`
        (
	    VendorID FLOAT64,
	    lpep_pickup_datetime TIMESTAMP,
	    lpep_dropoff_datetime TIMESTAMP,
	    passenger_count FLOAT64,
	    trip_distance FLOAT64,
	    RatecodeID FLOAT64,
	    store_and_fwd_flag STRING,
	    PULocationID INT64,
	    DOLocationID INT64,
	    payment_type FLOAT64,
	    fare_amount FLOAT64,
	    extra FLOAT64,
	    mta_tax FLOAT64,
	    tip_amount FLOAT64,
	    tolls_amount FLOAT64,
	    improvement_surcharge FLOAT64,
	    total_amount FLOAT64,
	    congestion_surcharge FLOAT64
        ) PARTITION BY DATE({DATETIME_COLUMN})
        """


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="Load-Green-GCS-To-BigQuery",
    description="Job to move data from website to local Postgresql DB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    catchup=True,
    tags=["GCS-Bucket-to-BigQuery"],
) as dag:

    start = EmptyOperator(task_id="start")

load_gcs_to_temp_table = BigQueryCreateExternalTableOperator(
    task_id=f"bq_{SERVICE}_{DATASET}_load_gcs_to_temp_table",
    destination_project_dataset_table=f"{DATASET}.{SERVICE}_external_table",
    schema_fields=[
        {"name": "VendorID", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "lpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "passenger_count", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "trip_distance", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "RatecodeID", "type": "FLOAT64", "mode": "NULLABLE"},
	    {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PULocationID", "type": "INT64", "mode": "NULLABLE"},
        {"name": "DOLocationID", "type": "INT64", "mode": "NULLABLE"},
        {"name": "payment_type", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "fare_amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "extra", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "mta_tax", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "tip_amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "tolls_amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "improvement_surcharge", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "total_amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "congestion_surcharge ", "type": "FLOAT64", "mode": "NULLABLE"},
    ],
    bucket=f"{SOURCE_BUCKET}",
    source_objects=[f"{SERVICE}/{SOURCE_OBJECT}"],
    source_format="CSV",
    autodetect=True,
    skip_leading_rows=1,
    gcp_conn_id="google_cloud_default",
    )

