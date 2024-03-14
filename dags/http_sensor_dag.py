# Imports 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
import requests

# DAG object
dag = DAG('http_sensor_dag', 
          description='DAG consumer',
          schedule= None,
          start_date=datetime(2024, 3, 12),
          catchup=False)

check_api = HttpSensor(
    task_id='check_api',
    http_conn_id='public_api',
    endpoint='entries',
    poke_interval=5,
    timeout=20,
    dag=dag)

def request_api():
    response = requests.get("https://api.publicapis.org/entries")
    print(response.text)


api_request = PythonOperator(
    task_id="api_request",
    python_callable=request_api,
    dag=dag
)

check_api >> api_request