import os
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.orm import sessionmaker
#MIGRATE LATER...

"""
def get_db_url():
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "postgres")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db   = os.getenv("PGDATABASE", "mydb")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

def get_engine(pool_size: int = 5):
    """"""SQLAlchemy engine for pandas .to_sql and DDL.""""""
    return create_engine(get_db_url(), pool_size=pool_size, max_overflow=10, future=True)

def get_conn():
    """"""psycopg2 raw connection for COPY/execute_values.""""""
    url = get_db_url().replace("+psycopg2", "")  # psycopg2 DSN format: postgresql://...
    return psycopg2.connect(url)
"""


DATABASE_URL = "sqlite:///./reports.sqlite"

# Create ONE engine and reuse it everywhere
engine = create_engine(
    DATABASE_URL,
    future=True,   # SQLAlchemy 2.x style
    echo=False     # set True to see SQL in logs
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# No declarative_base() since we're not using ORM models



