# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Import task group
from airflow.utils.task_group import TaskGroup

# DAG object
dag = DAG('triggered_dag', 
          description='DAG triggered by another DAG',
          schedule_interval=None,
          start_date=datetime(2024, 3, 12),
          catchup=False)

# Tasks
task1 = BashOperator(
    task_id='task_1',
    bash_command='sleep 5',
    dag=dag)

task2 = BashOperator(
    task_id='task_2',
    bash_command='sleep 5',
    dag=dag)


# Flow
task1 >> task2
