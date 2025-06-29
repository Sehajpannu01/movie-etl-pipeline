# scripts/extract_transform_load.py

import requests, os, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from database_setup import create_database_and_user, test_connection

# Load .env
load_dotenv()

API_KEY = os.getenv("TMDB_API_KEY")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")

def create_requests_session():
    """Creates a requests session with retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def extract(num_pages=5):
    print(f"Extracting data for {num_pages} pages from TMDb API...")
    all_movies = []
    base_url = "https://api.themoviedb.org/3/movie/popular"
    
    session = create_requests_session()

    for page in range(1, num_pages + 1):
        print(f"Fetching page {page}...")
        params = {
            'api_key': API_KEY,
            'language': 'en-US',
            'page': page
        }
        
        try:
            response = session.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            
            results = response.json().get('results', [])
            if not results:
                print(f"No more results found on page {page}. Stopping.")
                break
            all_movies.extend(results)
            time.sleep(0.2) # Be a good API citizen

        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data for page {page}: {e}")
            break
            
    print(f"Total movies extracted: {len(all_movies)}")
    return all_movies

def transform(movies):
    print("Transforming data...")
    df = pd.DataFrame([{
        'id': movie['id'],
        'title': movie['title'],
        'release_date': movie['release_date'],
        'vote_average': movie['vote_average']
    } for movie in movies])
    print("Data transformed.")
    return df

def setup_database():
    """Setup database and user if they don't exist."""
    print("Setting up database...")
    success = create_database_and_user()
    if success:
        print("Database setup completed.")
        return test_connection()
    else:
        print("Database setup failed. Please check your configuration.")
        return False

def load(df):
    print("Loading data into PostgreSQL...")
    
    # First, ensure database and user exist
    if not setup_database():
        raise Exception("Database setup failed. Cannot proceed with loading data.")
    
    # Create connection string
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    connection_string = f"postgresql://{DB_USER}:{DB_PASS}@{db_host}:{db_port}/{DB_NAME}"
    
    # Create engine and load data
    engine = create_engine(connection_string)
    df.to_sql('movies', engine, if_exists='replace', index=False)
    print("Data loaded to database.")

if __name__ == "__main__":
    movies = extract(num_pages=500)
    if movies:
        df = transform(movies)
        load(df)
    else:
        print("No movies were extracted. Halting pipeline.")