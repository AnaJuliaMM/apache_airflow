# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# DAG object
with DAG('with_dag', 
          description='This dag uses Pythons with operator',
          schedule_interval=None,
          start_date=datetime(2024, 3, 12),
          catchup=False) as dag:

    # Tasks
    task1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 5')

    task2 = BashOperator(
        task_id='task_2',
        bash_command='sleep 5')

    task3 = BashOperator(
        task_id='task_3',
        bash_command='sleep 5')

    # Flow (method 1) be two wise
    # task1 >> task2 >> task3

    # Flow (method 2) 
    task1.set_downstream(task2)
    task1.set_downstream(task3)