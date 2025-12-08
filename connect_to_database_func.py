# connect_to_database_func.py

import os
import psycopg2
from dotenv import load_dotenv
import logging

# Load variables from .env into environment
load_dotenv()

# Use a module-level logger (you already have logging_config for main parts)
logger = logging.getLogger("db_connection")


def connect_db():
    """
    Create and return a PostgreSQL connection using credentials from .env.

    Expected .env variables:
      PGHOST
      PGPORT
      PGDATABASE
      PGUSER
      PGPASSWORD
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            port=os.getenv("PGPORT", "5432"),
            dbname=os.getenv("PGDATABASE", "postgres"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "postgres"),
        )
        # You’re explicitly calling conn.commit() in your code,
        # so we keep autocommit disabled (default = False).
        logger.debug("Database connection established successfully.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}", exc_info=True)
        raise
