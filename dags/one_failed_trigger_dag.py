# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# DAG object
with  DAG('trigger_rule_dag', 
          description='This DAG handles trigger rules',
          schedule_interval=None,
          start_date=datetime(2024, 3, 12),
          catchup=False) as dag:

    # Tasks
    task1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 5')

    task2 = BashOperator(
        task_id='task_2',
        bash_command='exit 1')

    task3 = BashOperator(
        task_id='task_3',
        bash_command='sleep 5',
        trigger_rule='one_failed'
        )

    # Flow
    [task1, task2] >> task3