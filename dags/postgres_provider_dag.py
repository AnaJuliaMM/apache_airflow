from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests

# Functions
def print_rows(ti):
    rows = ti.xcom_pull(task_ids='select_data')
    print('Resultado da consulta:')
    for row in rows:
        print(row)



# DAG object
dag = DAG('postgres_provider_dag', 
          description='DAG that interact with local postgres conteiner',
          schedule= None,
          start_date=datetime(2024, 3, 12),
          catchup=False)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='airflow_postgres',
    sql="""
    CREATE TABLE IF NOT EXISTS IDS (id INT);
    """,
    dag=dag
    )

insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='airflow_postgres',
    sql="""
    insert into ids values(1);
    """,
    dag=dag
    )

select_data = PostgresOperator(
    task_id='select_data',
    postgres_conn_id='airflow_postgres',
    sql="""
    select * from ids;
    """,
    dag=dag
    )

print_rows = PythonOperator(
     task_id='print_rows',
     python_callable=print_rows,
     provide_context=True,
     dag=dag
)

create_table >>  insert_data >> select_data >> print_rows

