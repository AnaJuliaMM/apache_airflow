# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


# DAG object
dag = DAG('pool_dag', 
          description='Exploring Airflow pools',
          schedule_interval= None,
          start_date=datetime(2024, 3, 12),
          default_view='graph')

ask1 = BashOperator(
    task_id='task_1',
    bash_command='sleep 5',
    dag=dag,
    pool='my_pool')

task2 = BashOperator(
    task_id='task_2',
    bash_command='sleep 5',
    dag=dag,
    pool='my_pool',
    priority_weight=5)

task3 = BashOperator(
    task_id='task_3',
    bash_command='sleep 5',
    dag=dag,
    pool='my_pool')

task4 = BashOperator(
    task_id='task_4',
    bash_command='sleep 5',
    dag=dag,
    pool='my_pool',
    priority_weight=10)