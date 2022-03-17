# based on https://www.startdataengineering.com/post/how-to-submit-spark-jobs-to-emr-cluster-from-airflow/#further-reading # noqa

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from helpers.local_to_s3 import LocalToS3
from helpers.spark_steps import SparkSteps
from helpers.emr_cluster_config import EmrClusterConfig
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import (
    EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

load_dotenv()


aws_credentials = os.getenv('aws_credentials')
BUCKET_NAME = os.getenv('BUCKET_NAME')
wildfire_local_data = os.getenv('wildfire_local_data')
wildfire_s3_data = os.getenv('wildfire_s3_data')
weather_local_data = os.getenv('weather_local_data')
weather_s3_data = os.getenv('weather_s3_data')
local_script = os.getenv('local_script')
s3_script = os.getenv('s3_script')
s3_clean = os.getenv('s3_clean')
s3_logs_folder = os.getenv('s3_logs_folder')

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG(
    "spark_pre_process_airflow",
    default_args=default_args,
    schedule_interval="@once",
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

wildfire_data_to_s3 = PythonOperator(
    dag=dag,
    task_id="wildfire_data_to_s3",
    python_callable=LocalToS3._local_to_s3,
    op_kwargs={"filename": wildfire_local_data, "key": wildfire_s3_data,
               "bucket_name": BUCKET_NAME, },
)

weather_data_to_s3 = PythonOperator(
    dag=dag,
    task_id="weather_data_to_s3",
    python_callable=LocalToS3._local_to_s3,
    op_kwargs={"filename": weather_local_data, "key": weather_s3_data,
               "bucket_name": BUCKET_NAME, },
)

script_to_s3 = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=LocalToS3._local_to_s3,
    op_kwargs={"filename": local_script, "key": s3_script,
               "bucket_name": BUCKET_NAME, },
)

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=EmrClusterConfig.JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SparkSteps.SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "wildfire_s3_data": wildfire_s3_data,
        "s3_script": s3_script,
        "s3_clean": s3_clean,
    },
    dag=dag,
)

last_step = len(SparkSteps.SPARK_STEPS) - 1
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

start_data_pipeline >> [wildfire_data_to_s3, weather_data_to_s3, script_to_s3] >> create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_data_pipeline
