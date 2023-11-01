import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

logger = logging.getLogger(__name__)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
SOURCE_BUCKET = os.environ.get("GCP_GCS_BUCKET")


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2023, 9, 15),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="TaskGroup-Job-Example",
    description="Show how to use Xcom",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
    max_active_runs=1,
    catchup=True,
    tags=["Task-to-task"],
) as dag:


	@task(task_id="task_1", provide_context=True)
	def function_fun(**context):
		List_of_people_in_class  = ["Vanessa","Onu","Saeed","Peter","Rasheed", "Gospel","Bolu"]
		ti = context["task_instance"]
		ti.xcom_push(key="List_of_people_in_class", value=List_of_people_in_class )
		logger.info("Push THIS LIST  of student in class to the Xcom: %s", ",".join(List_of_people_in_class))

	@task(task_id="task_2", provide_context=True)
	def function_fun2(**context):
		ti=context["task_instance"]
		list_of_student=ti.xcom_pull(task_ids="task_1", key="List_of_people_in_class")
		list_of_student = list_of_student
		number_in_class = len(list_of_student)
		ti.xcom_push(key="number_in_class", value=number_in_class )
		logger.info("Push number of student in class to the Xcom: %s", number_in_class)

	@task(task_id="task_3", provide_context=True)
	def function_fun3(**context):
		ti=context["task_instance"]
		number_of_student_in_last_class=ti.xcom_pull(task_ids="task_2", key="number_in_class")
		number_of_students_today = 12
		if number_of_student_in_last_class > number_of_students_today:
			status = "Improvement In Attendance"
		else:
			status = "Steady Decline In Attendance"
		ti.xcom_push(key="Attendance_Condition", value=status)
		logger.info("Pushed Attendance Status in class to the Xcom: %s", status)

	start = EmptyOperator(task_id="start")

	with TaskGroup(
		   group_id = "task_group_1", prefix_group_id=False
	   ) as task_group_1:

			task_1 = function_fun()
			task_2 = function_fun2()

	task_1 >> task_2

	task_3 = function_fun3()

	end = EmptyOperator(task_id="end")

(
	start
    >> task_group_1
	>> task_3
	>> end
)
