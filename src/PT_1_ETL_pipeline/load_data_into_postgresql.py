import pandas as pd
import psycopg2

from psycopg2.extensions import connection as PGConnection
from psycopg2 import Error as PGError
from logger.logger_config import setup_logger

logger = setup_logger(__name__)

def sql_connection() -> PGConnection:
    """
    Establishes a connection to the PostgreSQL database using psycopg2.

    Returns:
        A psycopg2 database connection object.

    Raises:
        psycopg2.Error: If the connection cannot be established. 
    """
    try:
        
        conn = psycopg2.connect(
            host="localhost",
            database="test_db",
            user="postgres",
            password="postgres"
        )
        logger.info("Successfully connected to the database.")
        return conn

    except psycopg2.Error as e:
        logger.exception("Database connection failed.")
        raise
        

def fill_cars_table(
        df: pd.DataFrame | None                    
) -> str:
    """
    Creates the 'cars' table if it doesn't exist, and inserts records from a DataFrame.

    Args:
        df (pd.DataFrame | None): The input DataFrame containing car data. 
            Expected columns: ['year', 'make', 'model', 'size', 'kW', 'type'].

    Returns:
        str: Status message indicating success or failure.

    Raises:
        ValueError: If df is None or empty.
        psycopg2.Error: If a database error occurs.
    """

    if df is None or df.empty:
        logger.warning("Provided DataFrame is empty or None.")
        raise ValueError("DataFrame is empty or None")

    try:
        connection = sql_connection()

        with connection:
            with connection.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cars (
                        year INTEGER,
                        make TEXT,
                        model TEXT,
                        size TEXT, 
                        kW INTEGER,
                        type TEXT
                    )
                """)

                for _, row in df.iterrows():
                    cur.execute(
                        """
                        INSERT INTO cars (year, make, model, size, kW, type)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        tuple(row)
                    )

        logger.info("CSV successfully loaded into the 'cars' table.")

    except PGError as e:
        logger.exception("Failed to insert data into 'cars' table.")
        raise