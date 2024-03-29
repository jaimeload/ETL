from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def generate_random_number():
    import random
    number = random.randint(1, 100)
    print(f"Random number: {number}")
    return number

def square_number(**kwargs):
    number = kwargs['ti'].xcom_pull(task_ids='generate_random_number')
    squared_number = number ** 2
    print(f"Square of {number} is: {squared_number}")

def fetch_weather(location):
    response = requests.get(f"https://goweather.herokuapp.com/weather/{location}")
    print(response.text)

default_args = {
    'start_date': datetime(2021, 1, 1),
    'schedule_interval': '@daily',
    'catchup': False
}

with DAG("etl6", default_args=default_args) as dag:
    generate_random = PythonOperator(
        task_id='generate_random_number',
        python_callable=generate_random_number
    )

    print_random = BashOperator(
        task_id='print_random_number',
        bash_command='echo "{{ task_instance.xcom_pull(task_ids=\'generate_random_number\') }}"'
    )

    square_random = PythonOperator(
        task_id='square_number',
        python_callable=square_number
    )

    fetch_weather_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather,
        op_kwargs={"location": "Miass"}
    )

    generate_random >> print_random
    generate_random >> square_random
    square_random >> fetch_weather_task
