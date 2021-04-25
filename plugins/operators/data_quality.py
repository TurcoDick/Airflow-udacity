from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        logging.info(f"\n\n Starting the data quality process. \n\n")
        
        pg_hook = PostgresHook(postgres_conn_id = self.postgres_conn_id)
        
        dq_checks = [
            {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result':0},
            {'check_sql': "SELECT COUNT(*) FROM songplays WHERE userid is null", 'expected_result':0}
        ]
        
        failing_tests = []
        
        for check in dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
 
            records = pg_hook.get_records(sql)[0]
    
            error_count = 0
 
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
 
            if error_count > 0:
                self.log.info('SQL Tests failed')
                self.log.info(failing_tests)
                raise ValueError('Data quality check failed')
            
            if error_count == 0:
                self.log.info('SQL Tests Passed')
                
        logging.info(f"\n\n Finishing the data quality process. \n\n")