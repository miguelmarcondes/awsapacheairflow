from datetime import datetime, timedelta, timezone
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (CreatedTableOperator, StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime.utcnow(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG('udac_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreatedTableOperator(
    task_id='create_tables',
    dag=dag,
    create_table_dict=SqlQueries.create_table_dict,
    redshift_conn_id='redshift'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_event',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    file_format='JSON',
    table='staging_events',
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    file_format='JSON',
    table='staging_songs',
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql=SqlQueries.songplay_table_insert
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table_list=['songplays', 'artists', 'songs', 'users', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables

create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]

stage_events_to_redshift >> [load_songplays_table, load_users_dimension_table]
stage_songs_to_redshift >> [load_songplays_table, load_artist_dimension_table, load_song_dimension_table]

load_songplays_table >> load_time_dimension_table
[load_artist_dimension_table, load_song_dimension_table, load_users_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator