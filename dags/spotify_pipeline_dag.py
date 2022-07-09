from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime
from spotify_pipeline.spotify_pipeline_functions import spotify_pipeline

with DAG(
    "spotify_pipeline_dag",
    start_date=datetime(year=2022, month=7, day=7, tz="UTC"),
    schedule_interval="@daily"
) as dag:
    op = PythonOperator(
        task_id="spotify_pipeline",
        python_callable=spotify_pipeline
    )

    op
