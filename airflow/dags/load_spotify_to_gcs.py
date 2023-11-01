import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from spotify_operator import SpotifyToGCSOperators


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2023, 8, 28),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DESTINATION_BUCKET = os.environ.get("GCP_GCS_BUCKET")
ENDPOINT = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
DESTINATION_PATH = 'spotify_data_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv'

with DAG(
    dag_id="LoadSpotifyDataToGCS",
    description="Job to move data from website to local Postgresql DB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    catchup=True,
    tags=["Spotify-to-GCS-Bucket"],
) as dag:
    start = EmptyOperator(task_id="start")

    download_to_gcs= SpotifyToGCSOperators(
        task_id="download_to_gcs",
        days_ago=7,
        destination_path=DESTINATION_PATH,
        destination_bucket=DESTINATION_BUCKET,
        gcp_conn_id = "google_cloud_default",
        spotify_conn_id= "spotify_conn_id",
    )

    end = EmptyOperator(task_id="end")

    start >> download_to_gcs >> end