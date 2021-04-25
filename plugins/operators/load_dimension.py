from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
#     template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 table = "",
                 sql = "",
                 postgres_conn_id = "",
                 reload_data = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.reload_data = reload_data

    def execute(self, context):
        
        if self.reload_data:
            sql_complete = "INSERT INTO {} {}".format(self.table, self.sql)
        else:
            sql_complete = "DELETE FROM {}".format(self.sql)
        
        logging.info(f"\n\n Starting the load dimension the {self.table} table. \n\n")
        
        pg_hook = PostgresHook(postgres_conn_id = self.postgres_conn_id)
        sql_complete = "INSERT INTO {} {}".format(self.table, self.sql)
        logging.info(f"\n\n Finishing the load dimension the {self.table} table. \n\n")
        pg_hook.run(sql_complete)
        
        
