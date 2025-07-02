import requests, os, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from dotenv import load_dotenv

# Load .env
load_dotenv()

API_KEY = os.getenv("TMDB_API_KEY")

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

def extract(num_pages=2):  # Only 2 pages for quick testing
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
            time.sleep(0.2)

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
    
    print("Original dtypes:")
    print(df.dtypes)
    print("\n" + "="*50 + "\n")
    
    # Convert release_date to datetime
    print("Converting release_date to datetime...")
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

    # Checking for null values
    print("Checking for null values...")
    print(df.isnull().sum())
    print("\n" + "="*30 + "\n")

    # Drop rows with null values
    print("Dropping rows with null values...")
    df = df.dropna()

    # Print dtypes after transformation
    print("Final dtypes after transformation:")
    print(df.dtypes)
    print("\n" + "="*50 + "\n")
    
    # Show sample data
    print("Sample data:")
    print(df.head())
    
    print("Data transformed.")
    return df

if __name__ == "__main__":
    movies = extract(num_pages=2)  # Only 2 pages for quick testing
    if movies:
        df = transform(movies)
        print(f"\nFinal DataFrame shape: {df.shape}")
    else:
        print("No movies were extracted. Halting pipeline.") 