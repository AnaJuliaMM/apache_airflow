from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import random


# DAG object
dag = DAG('postgres_hook_dag', 
          description='DAG that interact with postgres using Postgre Hook',
          schedule= None,
          start_date=datetime(2024, 3, 12),
          catchup=False)

def create_table():
    pg_hook =PostgresHook(postgres_conn_id='airflow_postgres')
    pg_hook.run('CREATE TABLE IF NOT EXISTS AGES(id serial, age INT);', autocommit=True)

def insert_data():
    pg_hook =PostgresHook(postgres_conn_id='airflow_postgres')
    age = random.randint(1,120)
    pg_hook.run(f'INSERT INTO AGES(age) values ({age});', autocommit=True)

def select_data(ti):
    pg_hook =PostgresHook(postgres_conn_id='airflow_postgres')
    records = pg_hook.get_records('select * from AGES;')
    ti.xcom_push(key='select_data', value=records)

def print_rows(ti):
    rows = ti.xcom_pull(key='select_data', task_ids='select_data')
    print(f'Resultado da consulta:')
    for row in rows:
        print(row)

create_table = PythonOperator(
     task_id='create_table',
     python_callable=create_table,
     dag=dag
)

insert_data = PythonOperator(
     task_id='insert_data',
     python_callable=insert_data,
     dag=dag
)

select_data = PythonOperator(
     task_id='select_data',
     python_callable=select_data,
     provide_context=True,
     dag=dag
)

print_rows = PythonOperator(
     task_id='print_rows',
     python_callable=print_rows,
     provide_context=True,
     dag=dag
)

create_table >>  insert_data >> select_data >> print_rows
