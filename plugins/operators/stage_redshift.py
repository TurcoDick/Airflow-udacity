from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class StageToRedshiftOperator(BaseOperator):
   
    ui_color = '#358140'
    
#     template_fields = ("s3_key",)
    
    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
        COMPUPDATE {}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 region = "",
                 json = "",
                 compupdate = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.region = region
        self.json = json
        self.compupdate = compupdate

    def execute(self, context):
        logging.info(f"\n\n Starting the load to redshift the {self.table} table. \n\n")
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://udacity-dend/{}".format(self.s3_bucket)
        
        formatted_sql = self.COPY_SQL.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key, 
            self.region,
            self.json,
            self.compupdate
        )
        logging.info(f"\n\n Finishing the load to redshift the {self.table} table. \n\n")
        
        redshift_hook.run(formatted_sql)
                