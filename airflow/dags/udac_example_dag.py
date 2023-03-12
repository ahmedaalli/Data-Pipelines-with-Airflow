from datetime import datetime, timedelta
import os
from airflow.operators import PostgresOperator
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries
from airflow import conf

default_args = {
    "owner": "udacity",
    "start_date": datetime(2019, 1, 12),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "depends_on_past": False,
}

dag = DAG(
    "udac_example_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@hourly",
    catchup=False,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

queries = open("/home/workspace/airflow/create_tables.sql", "r").read()


create_all_tables = PostgresOperator(
    task_id="create_all_tables", postgres_conn_id="redshift", sql=queries, dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    table="staging_events",
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="s3://udacity-dend/log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    dag=dag,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    table="staging_songs",
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="s3://udacity-dend/song_data/A/A/A",
    json_path="auto",
    dag=dag,
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift",
    dag=dag,
    append_only=False,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    table="users",
    sql=SqlQueries.user_table_insert,
    redshift_conn_id="redshift",
    dag=dag,
    append_only=False,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    table="songs",
    sql=SqlQueries.song_table_insert,
    redshift_conn_id="redshift",
    append_only=False,
    dag=dag,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    redshift_conn_id="redshift",
    append_only=False,
    dag=dag,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    table="time",
    sql=SqlQueries.time_table_insert,
    redshift_conn_id="redshift",
    append_only=False,
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    redshift_conn_id="redshift",
    data_quality_check=[
        {"data_check_sql": "select count(*) from songs", "expected_value": 54},
        {"data_check_sql": "select count(*) from artists", "expected_value": 94},
    ],
    dag=dag,
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)


start_operator >> create_all_tables
(
    create_all_tables
    >> [stage_events_to_redshift, stage_songs_to_redshift]
    >> load_songplays_table
)
(
    load_songplays_table
    >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]
    >> run_quality_checks
)
run_quality_checks >> end_operator
