import psycopg2
from sqlalchemy import create_engine

CONNECTION_STRING = "postgresql://ndp-public:XKcBHaU1@awesome-hw.sdsc.edu:5432/nourish"


def get_psycopg2_connection():
    """Creates and returns a psycopg2 connection to PostgreSQL."""
    try:
        connection = psycopg2.connect(CONNECTION_STRING)
        return connection
    except Exception as error:
        print(f"Error connecting to PostgreSQL with psycopg2: {error}")
        return None


def get_sqlalchemy_engine():
    """Creates and returns an SQLAlchemy engine to connect to PostgreSQL."""
    try:
        engine = create_engine(CONNECTION_STRING)
        return engine
    except Exception as error:
        print(f"Error connecting to PostgreSQL with SQLAlchemy: {error}")
        return None
