import psycopg2

from psycopg2.extensions import connection as PGConnection
from psycopg2 import Error as PGError
from logger.logger_config import setup_logger

logger = setup_logger(__name__)


def sql_connection() -> PGConnection:
    """
    Establishes a connection to the PostgreSQL database.

    This function attempts to create a connection to the PostgreSQL database using 
    predefined connection parameters (host, database name, user, and password).
    If the connection is successful, it returns a `psycopg2` connection object.
    If the connection fails, the error is logged and re-raised.

    Returns:
        PGConnection: An active connection object to the PostgreSQL database.

    Raises:
        psycopg2.Error: If the connection to the database cannot be established.

    Logging:
        - Logs a success message upon successful connection.
        - Logs an exception and stack trace if the connection fails.
    
    Notes:
        - Ensure that the `logger` object is properly configured before calling this function.
        - This function does not close the connection; the caller is responsible for doing so.
    """
    try:
        
        connection = psycopg2.connect(
            host="localhost",
            database="test_db",
            user="postgres",
            password="postgres"
        )
        logger.info("Successfully connected to the database.")
        return connection

    except psycopg2.Error as e:
        logger.exception("Database connection failed.")
        raise


def fill_continent_table(
          continent_name: str | None 
) -> int:
    """
    Creates the 'continent' table if it doesn't exist and inserts a new continent.

    This function:
    1. Establishes a connection to the PostgreSQL database.
    2. Ensures that the 'continent' table exists (creates it if necessary).
    3. Inserts a new continent name into the table.
    4. Returns the generated primary key (`continent_id`) of the inserted record.

    Args:
        continent_name (str | None): The name of the continent to be inserted.
            Should be a non-empty string or None (if optional logic is added later).

    Returns:
        int: The `continent_id` of the inserted continent.

    Raises:
        PGError: If a database error occurs during table creation or data insertion.

    Logging:
        - Logs a success message if data is inserted successfully.
        - Logs an error message with stack trace if insertion fails.

    Notes:
        - Requires the `sql_connection()` function to return an active connection.
        - The caller must ensure that `continent_name` is valid and not a duplicate,
          unless the database schema or constraints handle this.
        - The database connection and cursor are properly closed using context managers.
    """
    try:
        connection = sql_connection()

        with connection:
            with connection.cursor() as cur:

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS continent(
                        continent_id SERIAL PRIMARY KEY,
                        continent_name VARCHAR(200) NOT NULL
                    )      
                """)

                cur.execute(
                        """
                        INSERT INTO continent (continent_name) 
                        VALUES (%s) 
                        RETURNING continent_id """,
                        (continent_name,)
                )
                logger.info("Data successfully loaded into the 'continent' table.")

                continent_id = cur.fetchone()[0]
                return continent_id
            
    except PGError as e:
        logger.exception("Failed to insert data into 'cars' table.")
        raise
        

def fill_country_table(
          continent_id: int | None, 
          country_name: str | None,
          capital: str | None,
          area: int | None,
          population: int | None      
) -> int:
    """
    Creates the 'country' table if it doesn't exist and inserts a new country record.

    This function connects to the PostgreSQL database and:
    1. Ensures the 'country' table exists, creating it if necessary with the following fields:
        - country_id: auto-incrementing primary key
        - continent_id: foreign key referencing 'continent.continent_id'
        - country_name: name of the country
        - capital: capital city of the country
        - area: total area of the country (integer)
        - population: population of the country (integer)
    2. Inserts a new record into the 'country' table using the provided parameters.
    3. Returns the `country_id` of the newly inserted row.

    Args:
        continent_id (int | None): ID of the continent the country belongs to (must exist in 'continent' table).
        country_name (str | None): Name of the country.
        capital (str | None): Capital city of the country.
        area (int | None): Area of the country in square kilometers.
        population (int | None): Population of the country.

    Returns:
        int: The `country_id` of the newly inserted row.

    Raises:
        psycopg2.Error: If any database operation fails.
    
    Logging:
        - Logs a success message after successful insertion.
        - Logs an exception and stack trace if an error occurs.

    Notes:
        - The connection is managed using a context manager (`with connection:`),
          which ensures proper commit or rollback behavior.
        - The 'continent' table must exist and contain the referenced `continent_id`.
    """
    try:
        connection = sql_connection()
        
        with connection:
            with connection.cursor() as cur:

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS country(
                        country_id SERIAL PRIMARY KEY,
                        continent_id INTEGER NOT NULL,
                        country_name VARCHAR(200) NOT NULL,
                        capital VARCHAR(200) NOT NULL,
                        area INTEGER NOT NULL,
                        population INTEGER NOT NULL,
                        FOREIGN KEY (continent_id) REFERENCES continent(continent_id)
                            ON DELETE CASCADE
                    )      
                """)

                cur.execute(
                        """
                        INSERT INTO country (continent_id, country_name, capital, area, population) 
                        VALUES (%s, %s, %s, %s, %s) 
                        RETURNING country_id 
                        """,
                        (continent_id, country_name, capital, area, population,)
                )
                logger.info("Data successfully loaded into the 'country' table.")

                country_id = cur.fetchone()[0] 
                return country_id
        
    except PGError as e:
        logger.exception("Failed to insert data into 'cars' table.")
        raise


def fill_city_table(
          country_id: int | None, 
          city_name: str | None,   
) -> None:
    """
    Creates the 'city' table if it does not exist and inserts a new city record.

    This function performs the following steps:
    1. Establishes a connection to the PostgreSQL database using `sql_connection()`.
    2. Ensures that the `city` table exists, with columns:
        - `city_id`: primary key, auto-incremented.
        - `country_id`: foreign key referencing `country(country_id)`.
        - `city_name`: name of the city (non-null).
    3. Inserts a new city record using the provided `country_id` and `city_name`.
    4. Logs a success message upon completion.

    Args:
        country_id (int | None): The ID of the country the city belongs to.
        city_name (str | None): The name of the city to be inserted.

    Returns:
        None

    Raises:
        psycopg2.Error: If the database operation fails.
    
    Logging:
        - Logs successful insertion into the 'city' table.
        - Logs and re-raises any exceptions during the process.

    Notes:
        - `country_id` must exist in the `country` table to satisfy the foreign key constraint.
        - Cascading deletes are enabled via `ON DELETE CASCADE`, meaning if a referenced country is deleted, its cities will also be deleted.
    """
    try:
        connection = sql_connection()

        with connection:
            with connection.cursor() as cur:

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS city(
                        city_id SERIAL PRIMARY KEY,
                        country_id INTEGER NOT NULL,
                        city_name VARCHAR(200) NOT NULL,
                        FOREIGN KEY (country_id) REFERENCES country(country_id)
                            ON DELETE CASCADE
                    )      
                """)

                cur.execute(
                        """
                        INSERT INTO city (country_id, city_name) 
                        VALUES (%s, %s)
                        """,
                        (country_id, city_name,)
                )
                logger.info("Data successfully loaded into the 'city' table.")
           
    except PGError as e:
        logger.exception("Failed to insert data into 'cars' table.")
        raise

                
