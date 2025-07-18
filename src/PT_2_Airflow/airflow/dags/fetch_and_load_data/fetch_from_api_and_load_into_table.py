import json
import requests
import psycopg2

from psycopg2.extensions import connection as PGConnection
from psycopg2 import Error as PGError
from logger.logger_config import setup_logger

logger = setup_logger(__name__)

def fetch_data_from_api() -> list:
    """
    Fetches hourly weather data (temperature and relative humidity) from the Open-Meteo API
    for coordinates (latitude=49, longitude=32). If the request is successful (HTTP 200),
    the response is saved to a local file named 'weather_data.json' and returned as a JSON object.
    Returns None if the request fails.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 49,
        "longitude": 32,
        "hourly": "temperature_2m,relative_humidity_2m"
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:

        weather_data = response.json()

        with open("weather_data.json", "w", encoding='utf-8') as file:
            json.dump(weather_data, file, indent=4, ensure_ascii=False)

        return weather_data
    else:
        print(f"Request error: {response.status_code}")

        return None


def sql_connection() -> PGConnection:
    """
    Establishes a connection to a PostgreSQL database.

    Returns:
        object: A psycopg2 connection object to the PostgreSQL database.

    Raises:
        psycopg2.OperationalError: If the connection to the database fails.
    """
    try:

        connection = psycopg2.connect(
            host="host.docker.internal",
            port=5432,
            database="test_db",
            user="postgres",
            password="postgres"
        )
        logger.info("Successfully connected to the database.")
        return connection
    
    except psycopg2.Error as e:
        logger.exception("Database connection failed.")
        raise

def fill_weather_table():
    """
    Creates a 'weather_data' table (if it does not exist) and inserts JSON weather data.

    This function:
    1. Establishes a connection to the PostgreSQL database.
    2. Creates a table named 'weather_data' with the following structure:
        - id: auto-incrementing primary key (SERIAL).
        - data: JSONB field to store weather information.
    3. Fetches weather data from an external API using `fetch_data_from_api()`.
    4. Serializes the data to a JSON string.
    5. Inserts the JSON data into the 'weather_data' table.

    Returns:
        str: A success message if data was inserted successfully.

    Raises:
        psycopg2.DatabaseError: If any error occurs while interacting with the database.
        Exception: If fetching or inserting data fails for any reason.

    Notes:
        - Requires the `sql_connection()` function to return an active PostgreSQL connection.
        - Requires the `fetch_data_from_api()` function to return data compatible with `json.dumps()`.
    """
    try:
        connection = sql_connection()

        with connection:
            with connection.cursor() as cur:

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS weather_data(
                        id SERIAL PRIMARY KEY,
                        data JSONB
                    )      
                """)

                fd = fetch_data_from_api()
                json_str = json.dumps(fd)

                cur.execute(
                    "INSERT INTO weather_data (data) VALUES (%s)",
                    (json_str,)
                )

        logger.info("JSON successfully loaded into the 'weather_data' table.")

    except PGError as e:
        logger.exception("Failed to insert data into 'cars' table.")
        raise

   
