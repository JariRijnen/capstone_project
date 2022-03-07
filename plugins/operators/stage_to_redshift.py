from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class StageToRedshiftOperator(BaseOperator):
    """Operator to copy data from local files to staging tables in redshift."""

    ui_color = '#358140'

    csv_copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    DELIMITER ','
    EMPTYASNULL 
    ESCAPE REMOVEQUOTES
    IGNOREHEADER 1;
    """

    parquet_copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS PARQUET;
    """

    def __init__(self,
                 postgres_conn_id="",
                 aws_credentials_id="",
                 table="",
                 file_location="",
                 format="",
                 *args, **kwargs):
        """
        Args:
            postgres_conn_id (str): name of the redshift connection (set in Airflow)
            aws_credentials_id (str): name of the aws_credentials (set in Airflow)
            table (str): name of table of interest
            file_location (str): file_location
            format (str): format of input file (either csv or parquet)
            *args, **kwargs: additional variable for the Operator.
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.file_location = file_location
        self.format = format

    def execute(self, context):
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id,
                               region_name="eu-west-1",
                               resource_type='redshift')
        self.log.info(aws_hook.get_session())
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self.log.info("Staging data from local files to redshift")
        if self.format == 'csv':
            formatted_sql = StageToRedshiftOperator.csv_copy_sql.format(
                self.table,
                self.file_location,
                credentials.access_key,
                credentials.secret_key
            )
            self.log.info(formatted_sql)
            redshift.run(formatted_sql)

        elif self.format == 'parquet':
            formatted_sql = StageToRedshiftOperator.parquet_copy_sql.format(
                self.table,
                self.file_location,
                credentials.access_key,
                credentials.secret_key
            )
            self.log.info(formatted_sql)
            redshift.run(formatted_sql)
        else:
            self.log.info("No valid file format given:", self.format)
