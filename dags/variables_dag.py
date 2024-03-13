# Imports 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

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
dag = DAG('variable_dag', 
          description='DAG that use env variables ',
          default_args = default_args,
          schedule_interval= '@hourly',
          start_date=datetime(2024, 3, 12),
          default_view='graph',
          tags=['process', 'pipeline'])

def print_variable(**context):
    my_var = Variable.get('my_var')
    print(f' O valor da variável é {my_var}')



task1 = python_task = PythonOperator(
    task_id="task_2",
    python_callable= print_variable,
    dag=dag)



# Flow
task1
