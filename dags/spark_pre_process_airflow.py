# based on https://www.startdataengineering.com/post/how-to-submit-spark-jobs-to-emr-cluster-from-airflow/#further-reading # noqa

from datetime import datetime, timedelta
import dotenv

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import (
    EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

aws_credentials = 'aws_default'
BUCKET_NAME = "udacity-bucket-jari"
wildfire_local_data = "./data/wildfires/wildfires.csv"
wildfire_s3_data = "data/wildfires/wildfires.csv"
weather_local_data = "./data/US_weather/US_weather_all.csv"
weather_s3_data = "data/us_weather/US_weather_all.csv"
local_script = "./dags/scripts/wildfires_test_script.py"
s3_script = "scripts/wildfires_test_script.py"
s3_clean = "clean_data/"

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

#  spark steps to give to the EMR cluster
SPARK_STEPS = [
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.wildfire_s3_data }}",
                "--dest=/wildfires",
            ],
        },
    },
    {
        "Name": "Pre-Process wildfire data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}",
            ],
        },
    },
]

# configurations of EMR cluster
JOB_FLOW_OVERRIDES = {
    "Name": "Wildfire cluster",
    "ReleaseLabel": "emr-6.5.0",
    "LogUri": "s3://udacity-bucket-jari/logs",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "Ec2KeyName": "emr-key",
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)


start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

wildfire_data_to_s3 = PythonOperator(
    dag=dag,
    task_id="wildfire_data_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": wildfire_local_data, "key": wildfire_s3_data, },
)

weather_data_to_s3 = PythonOperator(
    dag=dag,
    task_id="weather_data_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": weather_local_data, "key": weather_s3_data, },
)

script_to_s3 = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_script, "key": s3_script, },
)

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "wildfire_s3_data": wildfire_s3_data,
        "s3_script": s3_script,
        "s3_clean": s3_clean,
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
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
