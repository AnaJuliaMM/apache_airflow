# Imports 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 3),
    'email': ['mmartinsanajulia@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

# DAG object
dag = DAG('email_dag', 
          description='DAG that sends email',
          default_args = default_args,
          schedule_interval= None,
          catchup=False,
          default_view='graph',
          tags=['process', 'pipeline'])

# Tasks
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
    bash_command='exit 1',
    dag=dag)

task5 = BashOperator(
    task_id='task_5',
    bash_command='sleep 1',
    dag=dag,
    trigger_rule='none_failed')

task6 = BashOperator(
    task_id='task_6',
    bash_command='sleep 1',
    dag=dag,
    trigger_rule='none_failed')

send_email = EmailOperator(
    task_id="email_task",
    to='mmartinsanajulia@gmail.com',
    subject="Hello from your Airflow! ERROR",
    html_content="""
        <h3>Ocorreu um erro na Dag</h3>
        <p>Dag: email_dag</p>
        <br/>
        <p>Regra de gatilho configurada para one_failed, dessa forma, alguma tarefa da DAG falhou até esse momento</p>

    """,
    dag=dag,
    trigger_rule='one_failed')

# Flow
[task1, task2] >> task3 >> task4 >> [send_email, task5, task6]
