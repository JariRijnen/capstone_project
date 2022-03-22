from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):
    """Operator to check if the quality of the loaded data at the end of the pipeline"""
    ui_color = '#89DA59'

    def __init__(self,
                 postgres_conn_id="",
                 tables=[],
                 *args, **kwargs):
        """
        Args:
            postgres_conn_id (str): name of the postgres connection
            tables (list): list of table names
            *args, **kwargs: additional variable for the Operator.
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.tables = tables

    def execute(self, context):
        """Two data quality checks are done for all given tables.
        The first check to see if there is any data in any of the tables.
        The second to check if there is no duplicate data in any of the tables."""
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        for table in self.tables:
            count_records = postgres.get_records(f"SELECT COUNT(*) FROM {table}")
            distinct_records = postgres.get_records(f"SELECT DISTINCT COUNT(*) FROM {table}")

            self.log.info((f'Table {table}: distinct records: {distinct_records[0][0]} '
                           f'all records: {count_records[0][0]}'))

            if count_records[0][0] < 1:
                raise ValueError(f"Data quality check 1 failed. {table} returned no results.")

            if distinct_records[0][0] < count_records[0][0]:
                raise ValueError(f"Data quality check 2 failed. {table} has duplicate rows.")

        self.log.info('DataQualityOperator executed')
