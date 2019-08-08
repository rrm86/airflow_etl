'''
This file
implements a airflow Dag

Author: Ronnald R. Machado
'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator    
from helpers import SqlQueries
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'ronnald',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}

dag = DAG('project5_1',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly')

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_source="s3://udacity-dend/log_data",
    json_paths="s3://udacity-dend/log_json_path.json",
    file_type="JSON"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_source="s3://udacity-dend/song_data/A/",
    json_paths="auto",
    file_type="JSON"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    query=SqlQueries.user_table_insert,
    mode='delete-load'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    query=SqlQueries.song_table_insert,
    mode='delete-load'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    query=SqlQueries.artist_table_insert,
    mode='delete-load'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    query=SqlQueries.time_table_insert,
    mode='delete-load'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    data_quality_query = "SELECT COUNT(*) FROM ",
    value=1
    tables=["staging_events","staging_songs",
            "songplays","users",
            "songs","artists","time"]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

dim_tables = [load_user_dimension_table, load_song_dimension_table, 
              load_artist_dimension_table, load_time_dimension_table]

load_songplays_table >> dim_tables >> run_quality_checks

run_quality_checks >> end_operator
