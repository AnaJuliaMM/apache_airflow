# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime


# DAG object
dag = DAG('dummy_dag', 
          description='Dummy DAG',
          schedule_interval=None,
          start_date=datetime(2024, 3, 12),
          default_view='graph',
          tags=['process', 'pipeline'])


task1 = BashOperator(
    task_id='task_1',
    bash_command='sleep 1',
    dag=dag)

task2 = BashOperator(
    task_id='task_2',
    bash_command='sleep 1',
    dag=dag)

task3 = BashOperator(
    task_id='task_3',
    bash_command='sleep 1',
    dag=dag)

task4 = BashOperator(
    task_id='task_4',
    bash_command='sleep 1',
    dag=dag)

task5 = BashOperator(
    task_id='task_5',
    bash_command='sleep 1',
    dag=dag)

dummy_task = DummyOperator(
    task_id="dummy_task",
    dag=dag
)

[task1, task2, task3] >> dummy_task >> [task4, task5]