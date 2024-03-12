# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
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
dag = DAG('default_args_dag', 
          description='DAG args defined with default_args ',
          default_args = default_args,
          schedule_interval= '@hourly',
          start_date=datetime(2024, 3, 12),
          default_view='graph',
          tags=['process', 'pipeline'])

# Tasks
task1 = BashOperator(
    task_id='task_1',
    bash_command='sleep 5',
    dag=dag, 
    retries=3)

task2 = BashOperator(
    task_id='task_2',
    bash_command='sleep 5',
    dag=dag)

task3 = BashOperator(
    task_id='task_3',
    bash_command='sleep 5',
    dag=dag)


# Flow
task1 >> task2 >> task3
