from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table = "",
                 sql = "",
                 postgres_conn_id = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        logging.info(f"\n\n Starting the load fact the {self.table} table. \n\n")
        
        pg_hook = PostgresHook(postgres_conn_id = self.postgres_conn_id)
        sql_complete = "INSERT INTO {} {}".format(self.table, self.sql)
        logging.info(f"\n\n Finishing the load fact the {self.table} table. \n\n")
        pg_hook.run(sql_complete)
