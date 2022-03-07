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
        """Two data quality checks are done for all given tables."""
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        for table in self.tables:
            records = postgres.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        
        self.log.info('DataQualityOperator executed')