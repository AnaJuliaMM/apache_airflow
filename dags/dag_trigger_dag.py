# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# Import task group
from airflow.utils.task_group import TaskGroup

# DAG object
dag = DAG('dag_trigger_dag', 
          description='This DAG has a task that trigger the execution of another dag',
          schedule_interval=None,
          start_date=datetime(2024, 3, 12),
          catchup=False)

# Tasks
task1 = BashOperator(
    task_id='task_1',
    bash_command='sleep 5',
    dag=dag)

task2 = TriggerDagRunOperator(
    task_id='task_2',
    trigger_dag_id='triggered_dag',
    dag=dag)


# Flow
task1 >> task2
