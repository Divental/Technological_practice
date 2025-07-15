from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sql_executor.fetch_data import sql_execution


default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id = "fetch_data_from_api",
    start_date = datetime(2025, 7, 14),
    schedule_interval="0 14 * * 7",
    default_args=default_args,
    catchup = False,
    description="Fetch data from API and load into PostgreSQL",
) as dag:
    
    fetch_api_data = PythonOperator(
        task_id="fetch_data_from_api_task",
        python_callable = sql_execution
    )
    
    fetch_api_data

