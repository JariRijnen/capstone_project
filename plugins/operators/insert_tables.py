from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class InsertRedshiftTablesOperator(BaseOperator):
    """Operator to drop tables in postgres."""

    ui_color = '#358140'

    def __init__(self,
                 postgres_conn_id="",
                 query_list=[],
                 *args, **kwargs):
        """
        Args:
            postgres_conn_id (str): name of the postgres connection (set in Airflow)
            query_list (list): query_list to execute
            *args, **kwargs: additional variable for the Operator.
        """

        super(InsertRedshiftTablesOperator, self).__init__(*args, **kwargs)
        self.query_list = query_list
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self.log.info("Altering column types in postgres table")

        for query in self.query_list:
            self.log.info(query)
            redshift.run(query)
