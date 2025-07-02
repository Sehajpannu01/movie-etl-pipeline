import psycopg2

# Connection parameters
conn_params = {
    'host': 'localhost',
    'port': 5432,
    'database': 'movie_db',
    'user': 'postgres',
    'password': 'root'
}

try:
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True  # Enable autocommit
    cursor = conn.cursor()
    
    print("🔧 Dropping existing table...")
    cursor.execute("DROP TABLE IF EXISTS movies CASCADE;")
    
    print("🔧 Creating new table with proper schema...")
    cursor.execute("""
        CREATE TABLE movies (
            id BIGINT,
            title TEXT,
            release_date TIMESTAMP,
            vote_average DOUBLE PRECISION,
            year INTEGER
        );
    """)
    
    print("🔧 Verifying schema...")
    cursor.execute("""
        SELECT column_name, data_type, ordinal_position
        FROM information_schema.columns 
        WHERE table_name = 'movies' AND table_schema = 'public'
        ORDER BY ordinal_position;
    """)
    
    columns = cursor.fetchall()
    print(f"\n✅ Table created with {len(columns)} columns:")
    for col_name, col_type, pos in columns:
        print(f"  {pos}. {col_name}: {col_type}")
    
    # Test query structure
    cursor.execute("SELECT * FROM movies LIMIT 0;")
    query_columns = [desc[0] for desc in cursor.description]
    print(f"\n✅ Query columns: {query_columns}")
    
    conn.close()
    print("\n🎉 Table recreation completed!")
    
except Exception as e:
    print(f"❌ Error: {e}") 