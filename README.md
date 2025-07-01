# 🎬 Movie ETL Pipeline

A comprehensive **Extract, Transform, Load (ETL)** pipeline that automatically collects movie data from The Movie Database (TMDb), processes it, and stores it in a PostgreSQL database. The pipeline uses Apache Airflow for orchestration and includes Power BI dashboards for data visualization.

## 📋 Table of Contents

- [🎯 What This Project Does](#-what-this-project-does)
- [🏗️ Architecture](#️-architecture)
- [🚀 Quick Start](#-quick-start)
- [📁 Project Structure](#-project-structure)
- [🔧 Configuration](#-configuration)
- [💻 Usage](#-usage)
- [📊 Data Pipeline](#-data-pipeline)
- [🐳 Docker Setup](#-docker-setup)
- [🔍 Testing](#-testing)
- [📈 Dashboards](#-dashboards)
- [🛠️ Troubleshooting](#️-troubleshooting)
- [🤝 Contributing](#-contributing)

## 🎯 What This Project Does

This pipeline automatically:

1. **🔍 Extracts** popular movie data from TMDb API (titles, ratings, release dates, etc.)
2. **🔧 Transforms** the data (cleans null values, creates new features like release year)
3. **💾 Loads** the processed data into a PostgreSQL database
4. **📊 Visualizes** the data using Power BI dashboards
5. **🤖 Automates** the entire process using Apache Airflow

**Perfect for:**
- Data engineering beginners learning ETL concepts
- Data analysts wanting movie industry insights
- Developers building data pipelines
- Anyone interested in movie data analysis

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   TMDb API      │───▶│  Apache Airflow  │───▶│  PostgreSQL     │
│  (Data Source)  │    │  (Orchestrator)  │    │   (Database)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Power BI       │
                       │  (Dashboards)    │
                       └──────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- **Docker & Docker Compose** (recommended for beginners)
- **Python 3.8+** (if running without Docker)
- **TMDb API Key** (free registration required)

### Option 1: Docker Setup (Recommended for Beginners)

1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd movie_etl_pipeline
   ```

2. **Get your TMDb API Key:**
   - Visit [TMDb](https://www.themoviedb.org/settings/api)
   - Register for a free account
   - Request an API key

3. **Configure environment variables:**
   - Copy `.env.example` to `.env` (if available)
   - Or create a `.env` file with:
   ```
   TMDB_API_KEY=your_api_key_here
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=root
   POSTGRES_DB=movie_db
   POSTGRES_HOST=postgres
   POSTGRES_PORT=5432
   ```

4. **Start the services:**
   ```bash
   cd docker
   docker-compose up -d
   ```

5. **Access the interfaces:**
   - **Airflow UI:** http://localhost:8080 (admin/admin)
   - **Database:** localhost:5432

### Option 2: Local Python Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up PostgreSQL database:**
   ```bash
   python scripts/database_setup.py
   ```

3. **Test the connection:**
   ```bash
   python scripts/test_connection.py
   ```

4. **Run the ETL pipeline:**
   ```bash
   python scripts/extract_transform_load.py
   ```

## 📁 Project Structure

```
movie_etl_pipeline/
│
├── 📂 dags/                    # Airflow DAG definitions
│   └── movie_etl_dag.py       # Main ETL pipeline DAG
│
├── 📂 scripts/                # Python scripts
│   ├── extract_transform_load.py  # Core ETL logic
│   ├── database_setup.py         # Database initialization
│   ├── test_connection.py        # Connection testing
│   ├── test_db.py               # Database tests
│   ├── test_transform.py        # Data transformation tests
│   └── fix_table.py            # Database maintenance
│
├── 📂 docker/                 # Containerization
│   └── docker-compose.yml    # Multi-service setup
│
├── 📂 dashboards/            # Business Intelligence
│   └── dashboards.pbix      # Power BI dashboard file
│
├── 📂 logs/                  # Application logs
├── 📂 notebooks/            # Jupyter notebooks (optional)
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the root directory:

```bash
# TMDb API Configuration
TMDB_API_KEY=your_tmdb_api_key_here

# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=movie_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# For database setup (superuser credentials)
POSTGRES_SUPERUSER=postgres
POSTGRES_SUPERUSER_PASSWORD=your_superuser_password
```

### Key Settings

- **`num_pages`** in ETL script: Controls how many pages of movies to fetch (500 pages ≈ 10,000 movies)
- **Airflow schedule**: Currently set to `@daily` - runs once per day
- **Database**: PostgreSQL with automatic setup and user creation

## 💻 Usage

### Running with Airflow (Recommended)

1. **Start services:**
   ```bash
   docker-compose up -d
   ```

2. **Access Airflow UI:**
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

3. **Enable the DAG:**
   - Find "movie_etl_pipeline" in the DAG list
   - Toggle it ON
   - Click "Trigger DAG" to run immediately

### Running Scripts Directly

```bash
# Test database connection
python scripts/test_connection.py

# Run full ETL pipeline
python scripts/extract_transform_load.py

# Test data transformations
python scripts/test_transform.py

# Set up database from scratch
python scripts/database_setup.py
```

## 📊 Data Pipeline

### 1. Extract Phase
- Fetches popular movies from TMDb API
- Handles API rate limiting and retries
- Collects: title, release date, vote average, movie ID

### 2. Transform Phase
- Converts dates to proper datetime format
- Removes records with missing data
- Creates new features (release year)
- Data quality checks and validation

### 3. Load Phase
- Automatically sets up PostgreSQL database
- Creates necessary tables and users
- Loads clean data with proper schema
- Handles data types and constraints

### Sample Data Schema

```sql
CREATE TABLE movies (
    id INTEGER PRIMARY KEY,
    title VARCHAR(255),
    release_date DATE,
    vote_average FLOAT,
    year INTEGER
);
```

## 🐳 Docker Setup

The project includes a complete Docker setup with:

- **PostgreSQL 13**: Database service
- **Apache Airflow 2.7.2**: Orchestration service
- **Automatic initialization**: Database and Airflow setup
- **Health checks**: Ensures services are ready
- **Volume mounting**: For logs and DAGs

### Docker Commands

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Rebuild services
docker-compose up --build -d
```

## 🔍 Testing

### Available Tests

```bash
# Test database connectivity
python scripts/test_connection.py

# Test data transformations
python scripts/test_transform.py

# Test database operations
python scripts/test_db.py
```

### Manual Testing

1. **API Connection**: Check if TMDb API key works
2. **Database Setup**: Verify PostgreSQL connection
3. **Data Quality**: Run transformation tests
4. **End-to-End**: Execute full pipeline

## 📈 Dashboards

The project includes Power BI dashboards (`dashboards/dashboards.pbix`) with:

- **Movie Trends**: Release patterns over time
- **Rating Analysis**: Vote averages and distributions
- **Popular Movies**: Top-rated and most recent films
- **Data Quality Metrics**: Pipeline monitoring

### Connecting to Power BI

1. Open `dashboards.pbix` in Power BI Desktop
2. Update data source connection to your PostgreSQL:
   - Server: `localhost:5432`
   - Database: `movie_db`
   - Username: `postgres`
   - Password: (your password)

## 🛠️ Troubleshooting

### Common Issues

#### 1. API Key Problems
```bash
# Error: "Invalid API key"
# Solution: Check your TMDb API key in .env file
```

#### 2. Database Connection Issues
```bash
# Error: "Connection refused"
# Solutions:
# - Ensure PostgreSQL is running
# - Check connection parameters in .env
# - Run: python scripts/test_connection.py
```

#### 3. Docker Issues
```bash
# Error: "Port already in use"
# Solution: Stop conflicting services or change ports in docker-compose.yml

# Error: "Service unhealthy"
# Solution: Check logs with: docker-compose logs service_name
```

#### 4. No Data in Database
```bash
# Check if ETL pipeline completed successfully
# Verify API key and network connectivity
# Run: python scripts/test_db.py
```

### Getting Help

1. **Check logs**: `logs/` directory or `docker-compose logs`
2. **Test connections**: Use provided test scripts
3. **Verify configuration**: Double-check `.env` file
4. **Database issues**: Run `database_setup.py`

## 🤝 Contributing

### Development Setup

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and test thoroughly
4. Submit a pull request

### Guidelines

- **Code Style**: Follow PEP 8 for Python
- **Testing**: Add tests for new features
- **Documentation**: Update README for significant changes
- **Commits**: Use clear, descriptive commit messages

### Areas for Contribution

- Additional data sources (other movie APIs)
- More sophisticated transformations
- Additional visualizations
- Performance optimizations
- Error handling improvements

---

## 📞 Support

- **Issues**: Create GitHub issues for bugs
- **Questions**: Use GitHub Discussions
- **Documentation**: Check this README first

## 📄 License

This project is open source. Feel free to use and modify as needed.

---

**Happy Data Engineering! 🚀**

*This pipeline demonstrates real-world ETL practices and is perfect for learning data engineering concepts while working with actual movie industry data.* 