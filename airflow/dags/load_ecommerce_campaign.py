import os
from datetime import timedelta
from pathlib import Path

from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from airflow.utils.task_group import TaskGroup
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
#from utils.alert_callbacks import error_notification


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BASE_PATH = Path(__file__).parents[1]
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
TAG="monthly_web_campaign_analytics"
ENV="prod"
tag="bigquery_to_bigquery"

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": "2018-12-31",
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with models.DAG(
    dag_id = f"{ENV}-{TAG}",
	description = "Run a weekly summary of campaign profitability",
	default_args=DEFAULT_ARGS,
	schedule_interval="@monthly",
    template_searchpath=f"{BASE_PATH}/dags/include/",
	max_active_runs=2,
	catchup=True,
	tags={tag},
) as dag:
	start = EmptyOperator(task_id="start")

	load_to_staging = BigQueryExecuteQueryOperator(
		task_id = "load_to_staging",
		sql="/sql/bigquery_to_bigquery/monthly_web_campaign_analytics.sql",
		use_legacy_sql=False,
	)

	with TaskGroup(
		group_id="load_final_table",
		prefix_group_id = False,
		default_args={"dbt_cloud_conn_id": "dbt_conn_prod" }
	) as load_final_table:

		run_dbt_job = DbtCloudRunJobOperator(
			task_id="run_dbt_job",
			dbt_cloud_conn_id="dbt_conn_prod" ,
			job_id="448011",
			account_id="212662",
		)

		watch_dbt_cloud_run = DbtCloudJobRunSensor(
			task_id = "watch_dbt_cloud_run",
			dbt_cloud_conn_id="dbt_conn_prod",
			run_id ="{{ ti.xcom_pull(task_ids='run_dbt_job') }}",
			account_id="212662",
			sla=timedelta(minutes=45),
		)

	run_dbt_job >> watch_dbt_cloud_run

	end = EmptyOperator(task_id="end")

	start >> load_to_staging  >> load_final_table >>  end
