#!/usr/bin/env python3

import os
import sys
import pandas as pd

# Set environment variables for local testing
os.environ['POSTGRES_HOST'] = 'localhost'
os.environ['POSTGRES_PORT'] = '5432'
os.environ['POSTGRES_USER'] = 'postgres'
os.environ['POSTGRES_PASSWORD'] = 'postgres'
os.environ['POSTGRES_DB'] = 'movie_db'

# Add parent scripts directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our enhanced functions
from extract_transform_load import extract, transform

def test_enhanced_features():
    """Test the enhanced ETL with new columns."""
    
    print("ETL Connection Testing - Analyzing ~200 Movies for Genre IDs...")
    print("=" * 65)
    
    # Extract 10 pages for connection testing (~200 movies)
    print("1. Extracting 10 pages from TMDB to test connection and genre analysis...")
    movies = extract(num_pages=500)
    
    if not movies:
        print("ERROR: Extraction failed!")
        return
    
    print(f"SUCCESS: Extracted {len(movies)} movies")
    
    # Show sample raw API data
    print("\n2. Sample raw API data:")
    sample_movie = movies[0]
    print(f"Title: {sample_movie.get('title')}")
    print(f"Popularity: {sample_movie.get('popularity')}")
    print(f"Genre IDs: {sample_movie.get('genre_ids')}")
    print(f"Original Language: {sample_movie.get('original_language')}")
    print(f"Release Date: {sample_movie.get('release_date')}")
    
    # Transform with enhanced features
    print("\n3. Transforming data with enhanced features...")
    df = transform(movies)
    
    print(f"SUCCESS: Transformed {len(df)} rows")
    print(f"New columns: {list(df.columns)}")
    
    # Show enhanced data sample (limited for large dataset)
    print("\n4. Enhanced data sample (first 5 movies):")
    print(df[['title', 'popularity', 'original_language', 'release_month', 'genres', 'year']].head())
    
    # GENRE ANALYSIS - Check for unknown genres
    print("\n5. GENRE ANALYSIS:")
    print("-" * 40)
    
    # Extract all unique genre IDs from raw data
    all_genre_ids = set()
    for movie in movies:
        genre_ids = movie.get('genre_ids', [])
        all_genre_ids.update(genre_ids)
    
    print(f"Total unique genre IDs found: {len(all_genre_ids)}")
    print(f"Genre IDs: {sorted(all_genre_ids)}")
    
    # Show genre ID to name mapping for found IDs
    print(f"\nGenre ID to Name mappings found in dataset:")
    genre_mapping = {
        28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
        99: "Documentary", 18: "Drama", 10751: "Family", 14: "Fantasy", 36: "History",
        27: "Horror", 10402: "Music", 9648: "Mystery", 10749: "Romance", 878: "Science Fiction",
        10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western"
    }
    
    for genre_id in sorted(all_genre_ids):
        genre_name = genre_mapping.get(genre_id, f"UNKNOWN({genre_id})")
        print(f"   {genre_id}: {genre_name}")
    
    # Check for unknown genres in converted data
    unknown_genres = []
    for genres_str in df['genres']:
        if 'Unknown(' in genres_str:
            unknown_genres.append(genres_str)
    
    if unknown_genres:
        print(f"\nWARNING: FOUND {len(unknown_genres)} UNKNOWN GENRES:")
        for unknown in set(unknown_genres):
            print(f"   - {unknown}")
        print("ERROR: Missing genre IDs need to be added to mapping!")
    else:
        print("\nSUCCESS: All genres successfully mapped in current dataset!")
    
    # Show sample genre conversions
    print(f"\nSample genre conversions:")
    for i, row in df.head(3).iterrows():
        raw_ids = movies[i].get('genre_ids', [])
        converted = row['genres']
        print(f"   {raw_ids} -> {converted}")
    
    # Show data types
    print("\n6. Data types:")
    print(df.dtypes)
    
    print("\nEnhanced ETL test completed successfully!")
    print("New columns ready for Power BI analysis:")
    print("- popularity: For trending analysis")
    print("- original_language: For regional analysis") 
    print("- release_month: For seasonal trends")
    print("- genres: For content categorization")

if __name__ == "__main__":
    test_enhanced_features() 