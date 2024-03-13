# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime
import random

# DAG object
dag = DAG('branch_dag', 
          description='Exploring branch flow',
          schedule_interval= None,
          start_date=datetime(2024, 3, 12),
          default_view='graph')

def get_random_number():
    return random.randint(1,100)

def check_number(**context):
    number = context['task_instance'].xcom_pull(task_ids='get_random_number')
    if (number%2 == 0):
        return  'even_number'
    else:
        return 'odd_number' 



get_random_number = python_task = PythonOperator(
    task_id="get_random_number",
    python_callable= get_random_number ,
    dag=dag)

branch_task = BranchPythonOperator(
    task_id="check_number",
    python_callable= check_number,
    dag=dag,
    provide_context=True
)

even_number= BashOperator(
    task_id='even_number',
    bash_command="echo 'number is even'",
    dag=dag)

odd_number = BashOperator(
    task_id='odd_number',
    bash_command="echo 'number is odd'",
    dag=dag)


get_random_number >> branch_task >> [even_number, odd_number]