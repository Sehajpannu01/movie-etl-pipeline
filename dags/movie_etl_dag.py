from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow/scripts")

from extract_transform_load import extract, transform, load

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG("movie_etl_pipeline",
         schedule_interval="@daily",
         catchup=False,
         default_args=default_args,
         description="ETL pipeline for TMDb movies"
         ) as dag:

    extract_task = PythonOperator(
        task_id="extract_movies",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_movies",
        python_callable=lambda: transform(extract())
    )

    load_task = PythonOperator(
        task_id="load_movies",
        python_callable=lambda: load(transform(extract()))
    )

    extract_task >> transform_task >> load_task
