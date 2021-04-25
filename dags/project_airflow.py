from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)

from helpers.sql_queries import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
#     'start_date': datetime(2019, 1, 12),
    'start_date': datetime(2021, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
    'depends_on_past': False,
    'catchup_by_default': False
}

dag = DAG('project_airflow_v4',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval= '@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = 'staging_songs',
    s3_bucket = 'song_data',
    region = 'us-west-2',
    json = 'auto',
    compupdate = 'off',
    provide_context=True,
    dag = dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket = 'log_data',
    region = 'us-west-2',
    json = 's3://udacity-dend/log_json_path.json',
    compupdate = 'off',
    provide_context=True,
    dag = dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table = "songplays",
    sql = SqlQueries.songplay_table_insert,
    postgres_conn_id = "redshift",
    provide_context = True,
    dag = dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table = 'songs',
    sql = SqlQueries.song_table_insert,
    postgres_conn_id = "redshift",
    reload_data = False,
    provide_context=True,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    table = "users",
    sql = SqlQueries.user_table_insert,
    postgres_conn_id = "redshift",
    reload_data = False,
    provide_context=True,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table = "artists",
    sql = SqlQueries.artist_table_insert,
    postgres_conn_id = "redshift",
    reload_data = False,
    provide_context=True,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table = "time",
    sql = SqlQueries.time_table_insert, 
    postgres_conn_id = "redshift",
    reload_data = False,
    provide_context=True,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id= 'Run_data_quality_checks',
    postgres_conn_id = "redshift",
    provide_context=True,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator              >> stage_songs_to_redshift
start_operator              >> stage_events_to_redshift
stage_songs_to_redshift     >> load_songplays_table
stage_events_to_redshift    >> load_songplays_table
load_songplays_table        >> load_song_dimension_table
load_songplays_table        >> load_user_dimension_table
load_songplays_table        >> load_time_dimension_table
load_songplays_table        >> load_artist_dimension_table
load_song_dimension_table   >> run_quality_checks
load_user_dimension_table   >> run_quality_checks
load_time_dimension_table   >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
run_quality_checks          >> end_operator
