from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import pickle
import os
sys.path.append("/opt/airflow/scripts")

from extract_transform_load import extract, transform, load

default_args = {
    'start_date': datetime(2025, 6, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Simple wrapper functions that use file-based data sharing
def run_extract():
    movies = extract()
    print(f"Extract task completed: {len(movies)} movies extracted")
    
    # Save movies data to temporary file
    temp_dir = "/tmp"
    movies_file = os.path.join(temp_dir, "movies_raw.pkl")
    with open(movies_file, 'wb') as f:
        pickle.dump(movies, f)
    print(f"Movies data saved to {movies_file}")
    
    return len(movies)  # Return count for logging

def run_transform(**context):
    # Load movies data from file
    temp_dir = "/tmp"
    movies_file = os.path.join(temp_dir, "movies_raw.pkl")
    
    if not os.path.exists(movies_file):
        raise FileNotFoundError(f"Movies data file not found: {movies_file}")
    
    with open(movies_file, 'rb') as f:
        movies = pickle.load(f)
    
    print(f"Loaded {len(movies)} movies from file for transformation")
    df = transform(movies)
    print(f"Transform task completed: {len(df)} rows transformed")
    
    # Save transformed data to file
    df_file = os.path.join(temp_dir, "movies_transformed.pkl")
    with open(df_file, 'wb') as f:
        pickle.dump(df, f)
    print(f"Transformed data saved to {df_file}")
    
    return len(df)  # Return count for logging

def run_load(**context):
    # Load transformed data from file
    temp_dir = "/tmp"
    df_file = os.path.join(temp_dir, "movies_transformed.pkl")
    
    if not os.path.exists(df_file):
        raise FileNotFoundError(f"Transformed data file not found: {df_file}")
    
    with open(df_file, 'rb') as f:
        df = pickle.load(f)
    
    print(f"Load task starting with {len(df)} rows to load")
    load(df)
    
    # Clean up temporary files
    movies_file = os.path.join(temp_dir, "movies_raw.pkl")
    if os.path.exists(movies_file):
        os.remove(movies_file)
        print("Cleaned up movies_raw.pkl")
    
    if os.path.exists(df_file):
        os.remove(df_file)
        print("Cleaned up movies_transformed.pkl")

with DAG("movie_etl_pipeline",
         schedule_interval="@daily",
         catchup=False,
         default_args=default_args,
         description="ETL pipeline for TMDb movies"
         ) as dag:

    extract_task = PythonOperator(
        task_id="extract_movies",
        python_callable=run_extract
    )

    transform_task = PythonOperator(
        task_id="transform_movies",
        python_callable=run_transform
    )

    load_task = PythonOperator(
        task_id="load_movies",
        python_callable=run_load
    )

    extract_task >> transform_task >> load_task
