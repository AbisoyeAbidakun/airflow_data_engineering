import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
SERVICE = "green"
SOURCE_OBJECT = SERVICE +'_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv' #green_tripdata_2020-01.csv
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
SOURCE_BUCKET = os.environ.get("GCP_GCS_BUCKET")
DATASET="nyc"
DATETIME_COLUMN = "lpep_pickup_datetime"
FILE_FORMAT= "CSV"




DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
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

    load_gcs_to_bgquery =  GCSToBigQueryOperator(
        task_id = "load_gcs_to_bgquery",
        bucket=f"{SOURCE_BUCKET}", #BUCKET
        source_objects=[f"{SERVICE}/{SOURCE_OBJECT}"], # SOURCE OBJECT
        destination_project_dataset_table=f"{DATASET}.{SERVICE}_{DATASET}_data", # `nyc.green_dataset_data` i.e table name
        autodetect=True, #DETECT SCHEMA : the columns and the type of data in each columns of the CSV file
        write_disposition="WRITE_TRUNCATE", # command to update table from the  latest (or last row) row number upon every job run or task run
    )

    end = EmptyOperator(task_id="end")

    start >> load_gcs_to_bgquery >> end
