import requests
import pandas as pd
import json
import psycopg2

def fetch_data_from_api():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 49,
        "longitude": 32,
        "hourly": "temperature_2m,rain,relative_humidity_2m,dew_point_2m"
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        
        weather_data = response.json()
        
        return weather_data
        
    else:
        print(f"Помилка запиту: {response.status_code}")
        return None


def sql_connection():

    fd = fetch_data_from_api()

    with psycopg2.connect(
        host="localhost",
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

    return "json успішно завантажено у таблицю 'weather_data'"
