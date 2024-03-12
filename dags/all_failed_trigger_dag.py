# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# DAG object
with  DAG('all_failed_trigger_dag', 
          description='This DAG handle trigger rules',
          schedule_interval=None,
          start_date=datetime(2024, 3, 12),
          catchup=False) as dag:

    # Tasks
    task1 = BashOperator(
        task_id='task_1',
        bash_command='exit 1')

    task2 = BashOperator(
        task_id='task_2',
        bash_command='exit 1')

    task3 = BashOperator(
        task_id='task_3',
        bash_command='sleep 5',
        trigger_rule='all_failed'
        )

    # Flow
    [task1, task2] >> task3