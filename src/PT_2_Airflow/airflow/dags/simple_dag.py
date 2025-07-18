from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from fetch_and_load_data.fetch_from_api_and_load_into_table import fill_weather_table


default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id = "fetch_data_from_api",
    start_date = datetime(2025, 7, 14),
    schedule_interval = "0 14 * * 7",
    default_args = default_args,
    catchup = False,
    description="Fetch data from API and load into PostgreSQL",
) as dag:
    
    fetch_api_data = PythonOperator(
        task_id = "fetch_data_from_api_task",
        python_callable = fill_weather_table
    )
    
fetch_api_data

