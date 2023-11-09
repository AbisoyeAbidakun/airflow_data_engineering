import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator


logger = logging.getLogger(__name__)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")



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
    dag_id="Xcom-Job",
    description="Show how to use Xcom",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
    max_active_runs=1,
    catchup=True,
    tags=["Task-to-task"],
) as dag:

	start = EmptyOperator(task_id="start")

	@task(task_id="task_1", provide_context=True)
	def function_fun(**context):
		List_of_people_in_class  = ["Vanessa","Onu","Saeed","Peter","Rasheed"]
		ti = context["task_instance"]
		end_date = context["data_interval_end"]
		end_date_format = datetime.strptime(f"{end_date}","%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d")
		ti.xcom_push(key="List_of_people_in_class", value=List_of_people_in_class)
		ti.xcom_push(key="execution_end_date", value=end_date_format)
		logger.info("Push THIS LIST  of student in class to the Xcom: %s", ",".join(List_of_people_in_class))

	@task(task_id="task_2", provide_context=True)
	def function_fun2(**context):
		ti=context["task_instance"]
		list_of_student=ti.xcom_pull(task_ids="task_1", key="List_of_people_in_class")
		list_of_student = list_of_student
		number_in_class = len(list_of_student)
		ti.xcom_push(key="number_in_class", value=number_in_class )
		logger.info("Push number of student in class to the Xcom: %s", number_in_class)

	task_1 = function_fun()
	task_2 = function_fun2()

	end = EmptyOperator(task_id="end")

	(
	start
	>> task_1
	>> task_2
	>> end
	)
