from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator


from operators.redshift_cluster import RedshiftResumeClusterOperator
from operators.redshift_cluster import RedshiftPauseClusterOperator

from operators.drop_tables import DropRedshiftTablesOperator
from operators.create_tables import CreateRedshiftTablesOperator
from operators.stage_to_redshift import StageToRedshiftOperator
from operators.insert_tables import InsertRedshiftTablesOperator
from operators.data_quality import DataQualityOperator

from helpers.sql_queries.drop_tables import DropTables
from helpers.sql_queries.create_tables import CreateTables
from helpers.sql_queries.insert_tables import InsertTables


default_args = {
    'owner': 'Jari',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('wildfire_dag',
          default_args=default_args,
          description='Capstone project DAG',
          schedule_interval='@daily',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

resume_redshift = RedshiftResumeClusterOperator(
    task_id='Resume_redshift_cluster',
    dag=dag,
    cluster_identifier='redshift-cluster-1',
    aws_conn_id="aws_credentials"
)

drop_tables_if_exists = DropRedshiftTablesOperator(
    task_id='Drop_tables_if_exists',
    dag=dag,
    postgres_conn_id="redshift",
    query_list=DropTables.drop_tables
)

create_staging_tables = CreateRedshiftTablesOperator(
    task_id='Create_staging_tables_redshift',
    dag=dag,
    postgres_conn_id="redshift",
    query_list=CreateTables.create_staging_tables
)

create_tables = CreateRedshiftTablesOperator(
    task_id='Create_tables_redshift',
    dag=dag,
    postgres_conn_id="redshift",
    query_list=CreateTables.create_tables
)

stage_weather_to_redshift = StageToRedshiftOperator(
    task_id='Stage_weather',
    dag=dag,
    postgres_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_weather",
    file_location='s3://udacity-bucket-jari/data/us_weather/US_weather_all.csv',
    format='csv'
)

stage_wildfire_to_redshift = StageToRedshiftOperator(
    task_id='Stage_wildfire',
    dag=dag,
    postgres_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_wildfires",
    file_location='s3://udacity-bucket-jari/data/wildfires/wildfires.parquet',
    format='parquet'
)

insert_fact_tables_redshift = InsertRedshiftTablesOperator(
    task_id='Insert_fact_tables_redshift',
    dag=dag,
    postgres_conn_id="redshift",
    query_list=InsertTables.insert_fact_tables
)

insert_dimension_tables = InsertRedshiftTablesOperator(
    task_id='Insert_dimension_tables',
    dag=dag,
    postgres_conn_id="redshift",
    query_list=InsertTables.insert_dimension_tables
)

run_quality_checks = DataQualityOperator(
    task_id='Data_quality_check',
    dag=dag,
    postgres_conn_id="redshift",
    tables=['weather_measurements', 'weather_stations', 'date_table', 'time_table', 'us_state',
            'wildfires']
)

pause_redshift = RedshiftPauseClusterOperator(
    task_id='Pause_redshift_cluster',
    dag=dag,
    cluster_identifier='redshift-cluster-1',
    aws_conn_id="aws_credentials"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> resume_redshift

resume_redshift >> drop_tables_if_exists

drop_tables_if_exists >> create_staging_tables
create_staging_tables >> create_tables

create_tables >> stage_weather_to_redshift
create_tables >> stage_wildfire_to_redshift

stage_weather_to_redshift >> insert_fact_tables_redshift
stage_wildfire_to_redshift >> insert_fact_tables_redshift

insert_fact_tables_redshift >> insert_dimension_tables

insert_dimension_tables >> run_quality_checks

run_quality_checks >> pause_redshift

pause_redshift >> end_operator
