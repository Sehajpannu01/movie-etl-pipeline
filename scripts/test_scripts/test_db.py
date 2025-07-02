from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("postgresql://movie_user:movie_pass@localhost:5432/movie_db")
df = pd.read_sql("SELECT * FROM movies", engine)
print(df.head())
