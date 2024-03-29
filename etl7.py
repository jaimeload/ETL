from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime
import requests

def get_temperature(location):
   api_key = "e1cce65a79dc95bc95ec707ae22ccc2c"
   url = f"http://api.openweathermap.org/data/2.5/weather?q={location}&appid={api_key}&units=metric"
   response = requests.get(url)
   data = response.json()
   temperature = data["main"]["temp"]
   print(f"Температура в {location}: {temperature}°C")
   return temperature

def warm():
   print("тепло")

def cold():
   print("холодно")

default_args = {
   'owner': 'airflow',
   'start_date': datetime(2024, 3, 29),
   'retries': 1,
}

with DAG('etl_7', default_args=default_args, schedule_interval=None) as dag:

   get_temp = PythonOperator(
       task_id='get_temperature',
       python_callable=get_temperature,
       op_args=['Miass'],
   )

   branch_operator = BranchPythonOperator(
       task_id='kind_of_temperature',
       python_callable=lambda temp: 'warm' if temp > 15 else 'cold',
       op_args=[get_temp.output],
   )

   warm_task = PythonOperator(
       task_id='warm',
       python_callable=warm,
   )

   cold_task = PythonOperator(
       task_id='cold',
       python_callable=cold,
   )
   
   get_temp >> branch_operator >> [warm_task, cold_task]

