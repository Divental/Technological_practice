from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


with DAG(
    dag_id="example_python_operator_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    run_python = PythonOperator(
        task_id="run_my_python_task",
        python_callable=my_python_task
    )


