import psycopg2
import pandas as pd

# Connection parameters - exactly what Power BI and pgAdmin should use
conn_params = {
    'host': 'localhost',
    'port': 5432,
    'database': 'movie_db',
    'user': 'postgres',
    'password': 'root'
}

try:
    # Test connection
    conn = psycopg2.connect(**conn_params)
    print("✅ Connection successful!")
    
    # Check table schema directly
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM movies LIMIT 0;")  # Get column info without data
    columns_from_query = [desc[0] for desc in cursor.description]
    print(f"\n📊 Columns from direct query: {columns_from_query}")
    
    # Check information_schema
    cursor.execute("""
        SELECT column_name, data_type, ordinal_position
        FROM information_schema.columns 
        WHERE table_name = 'movies' AND table_schema = 'public'
        ORDER BY ordinal_position;
    """)
    
    schema_columns = cursor.fetchall()
    print(f"\n📋 Information schema ({len(schema_columns)} columns):")
    for col_name, col_type, pos in schema_columns:
        print(f"  {pos}. {col_name}: {col_type}")
    
    # Check table definition
    cursor.execute("SELECT pg_get_tabledef('public.movies'::regclass);")
    try:
        table_def = cursor.fetchone()
        print(f"\n🔧 Table definition: {table_def}")
    except:
        print("\n🔧 Could not get table definition")
    
    # Test pandas read
    try:
        df = pd.read_sql("SELECT * FROM movies LIMIT 1", conn)
        print(f"\n📈 Pandas columns: {list(df.columns)}")
        print(f"📈 Pandas shape: {df.shape}")
    except Exception as e:
        print(f"\n❌ Pandas error: {e}")
    
    conn.close()
    print("\n✅ Test completed!")
    
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print("\nCheck these settings:")
    for key, value in conn_params.items():
        print(f"  {key}: {value}") 