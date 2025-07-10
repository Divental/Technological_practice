import psycopg2


def sql_connection(df):

    with psycopg2.connect(
        host="localhost",
        database="test_db",
        user="postgres",
        password="postgres"
    ) as conn:  


        with conn.cursor() as cur:

            cur.execute("""
                CREATE TABLE IF NOT EXISTS cars(
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
                    "INSERT INTO cars (year, make, model, size, kW, type) VALUES (%s, %s, %s, %s, %s, %s)",
                    tuple(row)
                    )
        conn.commit()

    return "CSV успішно завантажено у таблицю 'cars'"
