# ====================================================================
# MOVIE ETL PIPELINE - AIRFLOW DAG
# ====================================================================
# This DAG (Directed Acyclic Graph) orchestrates our movie data pipeline
# DAG = A workflow definition that shows task dependencies and scheduling
# ====================================================================

# IMPORTS
# ====================================================================
from airflow import DAG                          # Core DAG class
from airflow.operators.python import PythonOperator  # For running Python functions
from datetime import datetime, timedelta        # For scheduling and dates
import sys
import pickle
import os

# Add our scripts directory to Python path so we can import our ETL functions
sys.path.append("/opt/airflow/scripts")
from extract_transform_load import extract, transform, load


# DAG CONFIGURATION
# ====================================================================
# These are default settings applied to all tasks in this DAG
default_args = {
    'owner': 'data_engineer',                    # Who owns this DAG
    'depends_on_past': False,                    # Don't wait for previous runs
    'start_date': datetime(2025, 6, 30),        # When this DAG can start running
    'email_on_failure': False,                  # Don't email on failures
    'email_on_retry': False,                    # Don't email on retries
    'retries': 2,                               # Retry failed tasks 2 times
    'retry_delay': timedelta(minutes=5),        # Wait 5 minutes between retries
}

# CONSTANTS
# ====================================================================
TEMP_DIR = "/tmp"                               # Directory for temporary files
RAW_DATA_FILE = "movies_raw.pkl"               # Filename for extracted data
TRANSFORMED_DATA_FILE = "movies_transformed.pkl"  # Filename for transformed data


# TASK FUNCTIONS
# ====================================================================
# These functions will be executed by Airflow tasks
# Each function represents one step in our ETL pipeline

def extract_movies_task():
    """
    EXTRACT TASK - Step 1 of ETL Pipeline
    
    What it does:
    - Calls our extract() function to get movie data from TMDb API
    - Saves the raw data to a temporary file for the next task
    - Returns the count of movies extracted for monitoring
    
    Why we save to file:
    - Airflow tasks run in separate processes
    - File sharing is the most reliable way to pass data between tasks
    """
    print("🎬 STARTING EXTRACT TASK - Getting movies from TMDb API...")
    
    # Call our extract function (gets data from API)
    movies_data = extract(num_pages=500)  # Extract 500 pages (≈10,000 movies)
    
    # Check if we got any data
    if not movies_data:
        raise ValueError("❌ No movies were extracted! Check API connection.")
    
    print(f"✅ Successfully extracted {len(movies_data)} movies")
    
    # Save extracted data to temporary file for next task
    raw_data_path = os.path.join(TEMP_DIR, RAW_DATA_FILE)
    with open(raw_data_path, 'wb') as file:
        pickle.dump(movies_data, file)
    
    print(f"💾 Raw data saved to: {raw_data_path}")
    return len(movies_data)  # Return count for Airflow monitoring


def transform_movies_task(**context):
    """
    TRANSFORM TASK - Step 2 of ETL Pipeline
    
    What it does:
    - Loads raw movie data from the file created by extract task
    - Calls our transform() function to clean and enrich the data
    - Saves transformed data to file for the load task
    
    **context parameter:
    - Airflow automatically passes context (task info, dates, etc.)
    - We use ** to accept it even though we don't use it here
    """
    print("🔄 STARTING TRANSFORM TASK - Cleaning and enriching movie data...")
    
    # Load raw data from extract task
    raw_data_path = os.path.join(TEMP_DIR, RAW_DATA_FILE)
    
    if not os.path.exists(raw_data_path):
        raise FileNotFoundError(f"❌ Raw data file missing: {raw_data_path}")
    
    # Load the pickled data
    with open(raw_data_path, 'rb') as file:
        movies_data = pickle.load(file)
    
    print(f"📂 Loaded {len(movies_data)} movies for transformation")
    
    # Transform the data (clean, add genres, create new columns)
    transformed_df = transform(movies_data)
    
    if transformed_df.empty:
        raise ValueError("❌ Transformation resulted in empty dataset!")
    
    print(f"✅ Successfully transformed data: {len(transformed_df)} rows ready")
    
    # Save transformed data for load task
    transformed_data_path = os.path.join(TEMP_DIR, TRANSFORMED_DATA_FILE)
    with open(transformed_data_path, 'wb') as file:
        pickle.dump(transformed_df, file)
    
    print(f"💾 Transformed data saved to: {transformed_data_path}")
    return len(transformed_df)


def load_movies_task(**context):
    """
    LOAD TASK - Step 3 of ETL Pipeline
    
    What it does:
    - Loads transformed data from file
    - Calls our load() function to insert data into PostgreSQL database
    - Cleans up temporary files
    - This is the final step of our ETL pipeline
    """
    print("📊 STARTING LOAD TASK - Inserting data into PostgreSQL...")
    
    # Load transformed data
    transformed_data_path = os.path.join(TEMP_DIR, TRANSFORMED_DATA_FILE)
    
    if not os.path.exists(transformed_data_path):
        raise FileNotFoundError(f"❌ Transformed data file missing: {transformed_data_path}")
    
    # Load the pickled DataFrame
    with open(transformed_data_path, 'rb') as file:
        transformed_df = pickle.load(file)
    
    print(f"📂 Loaded {len(transformed_df)} rows for database insertion")
    
    # Load data into PostgreSQL database
    load(transformed_df)
    print("✅ Successfully loaded data into PostgreSQL database!")
    
    # CLEANUP: Remove temporary files to save disk space
    cleanup_files = [
        os.path.join(TEMP_DIR, RAW_DATA_FILE),
        os.path.join(TEMP_DIR, TRANSFORMED_DATA_FILE)
    ]
    
    for file_path in cleanup_files:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"🧹 Cleaned up: {file_path}")
    
    print("🎉 ETL PIPELINE COMPLETED SUCCESSFULLY!")


# DAG DEFINITION
# ====================================================================
# This creates the actual DAG (workflow) that Airflow will execute

# Create the DAG object
dag = DAG(
    dag_id='movie_etl_pipeline',                # Unique name for this DAG
    default_args=default_args,                  # Apply our default settings
    description='Daily ETL pipeline to extract movie data from TMDb API, transform it, and load into PostgreSQL',
    schedule_interval='@daily',                 # Run once per day
    catchup=False,                             # Don't run for past dates
    max_active_runs=1,                         # Only one instance can run at a time
    tags=['etl', 'movies', 'tmdb', 'postgresql']  # Tags for organization
)

# TASK DEFINITIONS
# ====================================================================
# Create individual tasks using PythonOperator
# PythonOperator = Executes a Python function as an Airflow task

# Task 1: Extract movies from TMDb API
extract_task = PythonOperator(
    task_id='extract_movies',                   # Unique task name
    python_callable=extract_movies_task,       # Function to execute
    dag=dag,                                   # Which DAG this belongs to
    doc_md="""
    ### Extract Task
    This task extracts movie data from The Movie Database (TMDb) API.
    - Fetches ~10,000 movies from popular movies endpoint
    - Handles API rate limiting and errors
    - Saves raw data for transformation task
    """
)

# Task 2: Transform the extracted data
transform_task = PythonOperator(
    task_id='transform_movies',
    python_callable=transform_movies_task,
    dag=dag,
    doc_md="""
    ### Transform Task
    This task cleans and enriches the raw movie data.
    - Converts genre IDs to readable names
    - Creates additional date-based columns
    - Filters out movies with missing data
    - Prepares data for database loading
    """
)

# Task 3: Load data into PostgreSQL
load_task = PythonOperator(
    task_id='load_movies',
    python_callable=load_movies_task,
    dag=dag,
    doc_md="""
    ### Load Task
    This task loads the transformed data into PostgreSQL database.
    - Connects to PostgreSQL database
    - Replaces existing data with fresh data
    - Verifies successful data insertion
    - Cleans up temporary files
    """
)

# TASK DEPENDENCIES
# ====================================================================
# Define the order in which tasks should run
# >> means "then" - so extract_task >> transform_task means "extract_task then transform_task"

extract_task >> transform_task >> load_task

# This creates the flow: Extract → Transform → Load
# Each task waits for the previous one to complete successfully
