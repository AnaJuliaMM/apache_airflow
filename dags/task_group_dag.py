# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Import task group
from airflow.utils.task_group import TaskGroup

# DAG object
dag = DAG('task_group_dag', 
          description='This DAG has tasks separated in group',
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

# Create task group
task_group = TaskGroup("task_789", dag=dag)

task7 = BashOperator(
    task_id='task_7',
    bash_command='sleep 5',
    dag=dag,
    task_group = task_group)

task8 = BashOperator(
    task_id='task_8',
    bash_command='sleep 5',
    dag=dag, 
    task_group = task_group)

task9 = BashOperator(
    task_id='task_9',
    bash_command='sleep 5',
    dag=dag, 
    trigger_rule='one_failed',
    task_group = task_group)

# Flow
task1 >> task2
task3 >> task4
[task2, task4] >> task5 >> task6
task6 >> task_group