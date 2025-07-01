# scripts/extract_transform_load.py

import requests, os, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from database_setup import create_database_and_user, test_connection

# Load .env
load_dotenv()

API_KEY = os.getenv("TMDB_API_KEY")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")

def create_requests_session():
    """Creates a requests session with retry logic and SSL handling."""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # Suppress SSL warnings
    
    session = requests.Session()
    retry_strategy = Retry(
        total=3,  # Reduced retries to speed up
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def extract(num_pages=500):
    print(f"Extracting data for {num_pages} pages from TMDb API...")
    
    # Check if API key is available
    if not API_KEY:
        print("ERROR: TMDB_API_KEY not found in environment variables!")
        return []
    
    print(f"Using API key: {API_KEY[:10]}...{API_KEY[-4:]}")  # Show partial key for debugging
    
    # Detect if running in Docker/container environment
    is_docker = os.path.exists('/.dockerenv') or os.getenv('POSTGRES_HOST') == 'postgres'
    ssl_verify = not is_docker  # Disable SSL verification in Docker by default
    
    if is_docker:
        print("Detected Docker environment - SSL verification disabled by default")
    
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
            # Use SSL verification setting based on environment
            response = session.get(base_url, params=params, timeout=30, verify=ssl_verify)
            print(f"Response status code: {response.status_code}")
            
            if response.status_code == 401:
                print("ERROR: Invalid API key!")
                break
            elif response.status_code != 200:
                print(f"ERROR: HTTP {response.status_code}: {response.text}")
                break
                
            response.raise_for_status()
            
            results = response.json().get('results', [])
            if not results:
                print(f"No more results found on page {page}. Stopping.")
                break
            print(f"Successfully fetched {len(results)} movies from page {page}")
            all_movies.extend(results)
            time.sleep(0.2) # Be a good API citizen

        except requests.exceptions.SSLError as e:
            print(f"SSL Error on page {page}: {e}")
            print("Trying with SSL verification disabled...")
            try:
                response = session.get(base_url, params=params, timeout=30, verify=False)
                response.raise_for_status()
                results = response.json().get('results', [])
                if results:
                    print(f"Successfully fetched {len(results)} movies from page {page} (SSL disabled)")
                    all_movies.extend(results)
                    time.sleep(0.2)
                else:
                    print(f"No results on page {page}")
                    break
            except Exception as retry_e:
                print(f"Retry failed for page {page}: {retry_e}")
                break
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data for page {page}: {e}")
            break
            
    print(f"Total movies extracted: {len(all_movies)}")
    if len(all_movies) > 0:
        print("Sample movie titles:", [movie.get('title', 'Unknown') for movie in all_movies[:3]])
    return all_movies

def transform(movies):
    print("Transforming data...")
    
    # Handle empty movies list
    if not movies:
        print("WARNING: No movies to transform! Creating empty DataFrame.")
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=['id', 'title', 'release_date', 'vote_average', 'year'])
    
    df = pd.DataFrame([{
        'id': movie['id'],
        'title': movie['title'],
        'release_date': movie['release_date'],
        'vote_average': movie['vote_average']
    } for movie in movies])
    
    # Convert release_date to datetime
    print("Converting release_date to datetime...")
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

    # Checking for null values
    print("Checking for null values...")
    print(df.isnull().sum())

    # Drop rows with null values
    print("Dropping rows with null values...")
    df = df.dropna()

    # Print dtypes
    print("Dtypes:--------")
    print(df.dtypes)

    # Do feature engineering
    # Create year column from release_date
    print("Creating year column from release_date...")
    df['year'] = df['release_date'].dt.year
    
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
    
    # Handle empty DataFrame
    if df.empty:
        print("WARNING: No data to load! DataFrame is empty.")
        return
    
    # First, ensure database and user exist
    if not setup_database():
        raise Exception("Database setup failed. Cannot proceed with loading data.")
    
    # Create connection string
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    connection_string = f"postgresql://{DB_USER}:{DB_PASS}@{db_host}:{db_port}/{DB_NAME}"
    
    # Print info about the DataFrame we're loading
    print(f"DataFrame shape: {df.shape}")
    print(f"DataFrame columns: {list(df.columns)}")
    print("Sample data:")
    print(df.head())
    
    # Create engine with explicit connection management
    engine = create_engine(connection_string, echo=False)
    
    try:
        # Use explicit connection and transaction management
        with engine.begin() as connection:
            # Force table recreation to ensure correct schema including 'year' column
            df.to_sql('movies', connection, if_exists='replace', index=False)
            print(f"Successfully loaded {len(df)} rows to database with all columns including 'year'.")
            
            # Verify data was loaded by counting rows
            result = connection.execute(text("SELECT COUNT(*) FROM movies"))
            row_count = result.scalar()
            print(f"Verification: Database now contains {row_count} rows in movies table.")
            
    except Exception as e:
        print(f"Error loading data to database: {e}")
        raise
    finally:
        # Ensure engine is properly disposed
        engine.dispose()
        print("Database connection closed.")

if __name__ == "__main__":
    movies = extract(num_pages=500)
    if movies:
        df = transform(movies)
        load(df)
    else:
        print("No movies were extracted. Halting pipeline.")