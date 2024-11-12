from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define a simple Python function
def print_hello():
    print("Hello, Airflow!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='sample_hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval='@daily',  # Runs daily
    start_date=datetime(2024, 11, 10),
    catchup=False,
) as dag:

    # Define a Python task
    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )
