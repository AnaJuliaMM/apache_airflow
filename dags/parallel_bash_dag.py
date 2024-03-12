# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# DAG object
dag = DAG('parallel_first_bash_dag', 
          description='My fist parallel dag consist in bash commands executions in parallel',
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

task3 = BashOperator(
    task_id='task_3',
    bash_command='sleep 5',
    dag=dag)

# Flow
task1 >> [task2, task3]
