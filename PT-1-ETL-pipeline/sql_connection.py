import psycopg2


def sql_connection(df):
    """
    Connects to a PostgreSQL database and inserts data from 
        a pandas DataFrame into the 'cars' table.

    - Creates the 'cars' table if it does not already exist.
    - Inserts all rows from the provided DataFrame.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing car data 
            with the following columns: 
            ['year', 'make', 'model', 'size', 'kW', 'type']

    Returns:
        str: Success message if the operation completes.
    """
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
                    "INSERT INTO cars (year, make, model, size, kW, type) " \
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    tuple(row)
                    )
        conn.commit()

    return "CSV successfully loaded into the 'cars' table"
