# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# DAG object
dag = DAG('complex_flow_dag', 
          description='This DAG contains a complex flow that have to be break down in lines to be written',
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

task4 = BashOperator(
    task_id='task_4',
    bash_command='sleep 5',
    dag=dag)

task5 = BashOperator(
    task_id='task_5',
    bash_command='sleep 5',
    dag=dag)

task6 = BashOperator(
    task_id='task_6',
    bash_command='sleep 5',
    dag=dag)

task7 = BashOperator(
    task_id='task_7',
    bash_command='sleep 5',
    dag=dag)

task8 = BashOperator(
    task_id='task_8',
    bash_command='exit 1',
    dag=dag)

task9 = BashOperator(
    task_id='task_9',
    bash_command='sleep 5',
    dag=dag, 
    trigger_rule='one_failed')

# Flow
task1 >> task2
task3 >> task4
[task2, task4] >> task5 >> task6
task6 >> [task7, task8, task9]