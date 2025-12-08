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
      DB_HOST
      DB_PORT
      DB_NAME
      DB_USER
      DB_PASS
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            dbname=os.getenv("DB_NAME", "postgres"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "postgres"),
        )
        # You’re explicitly calling conn.commit() in your code,
        # so we keep autocommit disabled (default = False).
        logger.debug("Database connection established successfully.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}", exc_info=True)
        raise
