# scripts/database_setup.py

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_database_and_user():
    """
    Creates database and user if they don't exist.
    Requires connection to PostgreSQL as a superuser (usually 'postgres').
    """
    
    # Get environment variables
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_pass = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    
    # Superuser credentials (you'll need to set these)
    superuser = os.getenv("POSTGRES_SUPERUSER", "postgres")
    superuser_pass = os.getenv("POSTGRES_SUPERUSER_PASSWORD", "")
    
    print(f"Setting up database: {db_name}")
    print(f"Setting up user: {db_user}")
    
    try:
        # Connect as superuser to create database and user
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=superuser,
            password=superuser_pass
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if user exists
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (db_user,))
        user_exists = cursor.fetchone()
        
        if not user_exists:
            print(f"Creating user: {db_user}")
            cursor.execute(f"CREATE USER {db_user} WITH PASSWORD '{db_pass}'")
            print(f"User {db_user} created successfully")
        else:
            print(f"User {db_user} already exists")
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        db_exists = cursor.fetchone()
        
        if not db_exists:
            print(f"Creating database: {db_name}")
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"Database {db_name} created successfully")
        else:
            print(f"Database {db_name} already exists")
        
        # Grant privileges to user on database
        print(f"Granting privileges to {db_user} on {db_name}")
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {db_user}")
        
        # Connect to the specific database to grant schema privileges
        conn.close()
        
        # Connect to the specific database
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=superuser,
            password=superuser_pass
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Grant schema privileges
        cursor.execute(f"GRANT ALL ON SCHEMA public TO {db_user}")
        cursor.execute(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {db_user}")
        cursor.execute(f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {db_user}")
        cursor.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {db_user}")
        cursor.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {db_user}")
        
        print("Database setup completed successfully!")
        
    except psycopg2.Error as e:
        print(f"Database setup failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure PostgreSQL is running")
        print("2. Check your superuser credentials in .env file")
        print("3. Ensure superuser has CREATE USER and CREATE DATABASE privileges")
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()
    
    return True

def test_connection():
    """Test connection with the configured user and database."""
    
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_pass = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_pass
        )
        print(f"✅ Connection successful to {db_name} as {db_user}")
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    print("Database Setup Utility")
    print("=" * 30)
    
    success = create_database_and_user()
    if success:
        print("\nTesting connection...")
        test_connection() 