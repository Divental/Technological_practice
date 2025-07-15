import json
import requests
import psycopg2

def fetch_data() -> list:
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


def sql_execution() -> str:
    """
    Connects to a PostgreSQL database and inserts weather data fetched from an API
    into a table named 'weather_data'. If the table does not exist, it is created
    with a JSONB column to store the data. Returns a success message upon completion.
    """
    fd = fetch_data()

    with psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        database="test_db",
        user="postgres",
        password="postgres"
    ) as conn:
        with conn.cursor() as cur:

            cur.execute("""
                CREATE TABLE IF NOT EXISTS weather_data(
                    id SERIAL PRIMARY KEY,
                    data JSONB
                )      
            """)

            json_str = json.dumps(fd)

            cur.execute(
                "INSERT INTO weather_data (data) VALUES (%s)",
                (json_str,)
            )

        conn.commit()

    return "JSON successfully loaded into the 'weather_data' table"
