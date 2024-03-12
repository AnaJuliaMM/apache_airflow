# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 3),
    'email': ['ana_juh17@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': None,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

# DAG object
dag = DAG('xcom_dag', 
          description='DAG that use xcom to communicate between tasks ',
          default_args = default_args,
          schedule_interval= '@hourly',
          start_date=datetime(2024, 3, 12),
          default_view='graph',
          tags=['process', 'pipeline'])



def task_write(**kwarg):
    kwarg['ti'].xcom_push(key='value_xcom1', value="{name = 'Ana Julia'}")
 
task1 = python_task = PythonOperator(
    task_id="task_1",
    python_callable= task_write,
    dag=dag)


def task_read(**kwarg):
    value = kwarg['ti'].xcom_pull(key='value_xcom1')
    print(f'valor recuperado: {value}')


task2 = python_task = PythonOperator(
    task_id="task_2",
    python_callable= task_read,
    dag=dag)



# Flow
task1 >> task2 
