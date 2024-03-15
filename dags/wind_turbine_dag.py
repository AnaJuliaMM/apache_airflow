from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os

default_args = {
    'depends_on_past': False,
    'email': ['mmartinsanajulia@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}


#  schedule_interval="* /3 * * * *"
dag = DAG(
    'wind_turbine_dag',
    default_args=default_args,
    description='DAG monitore wind turbine data ',
    schedule_interval=None,
    start_date=datetime(2024, 3, 14),
    catchup=False,
    default_view='graph',
    doc_md=" # Monitoramente de dados de turbinas eólicas"
)
    
# Groups
group_check_temp = TaskGroup("group_check_temp", dag=dag)
group_postgres = TaskGroup("group_postgres", dag=dag)

# Tasks
file_sensor_task = FileSensor(
    task_id = 'file_sensor',
    filepath = Variable.get("path_wind_turbine"),
    fs_conn_id= 'fs_default',
    poke_interval = 10,
    dag = dag
)


def load_data(ti):
    with open(Variable.get("path_wind_turbine")) as file:
        data = json.load(file)
        # Arquivo aberto
        ti.xcom_push(key="idtemp", value=data['idtemp'])
        ti.xcom_push(key="powerfactor", value=data['powerfactor'])
        ti.xcom_push(key="hydraulicpressure", value=data['hydraulicpressure'])
        ti.xcom_push(key="temperature", value=data['temperature'])
        ti.xcom_push(key="timestamp", value=data['timestamp'])
    os.remove(Variable.get("path_wind_turbine"))


load_data_task = PythonOperator(
    task_id = 'load_data',
    python_callable = load_data,
    provide_context = True,
    dag = dag
)

create_table_task = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id='airflow_postgres',
    sql= """ CREATE TABLE IF NOT EXISTS WIND_TURBINE (
        idtemp varchar, 
        powerfactor varchar,
        hydraulicpressure varchar,
        temperature varchar,
        timestamp varchar    
    );""",
    task_group= group_postgres,
    dag = dag

)

insert_data_task = PostgresOperator(
    task_id = 'insert_data',
    postgres_conn_id='airflow_postgres',
    parameters=(
        '{{ ti.xcom_pull(task_ids="load_data", key="idtemp") }}',
        '{{ ti.xcom_pull(task_ids="load_data", key="powerfactor") }}',
        '{{ ti.xcom_pull(task_ids="load_data", key="hydraulicpressure")}}',
        '{{ ti.xcom_pull(task_ids="load_data", key="temperature") }}',
        '{{ ti.xcom_pull(task_ids="load_data", key="timestamp") }}',
    ),
    sql= """
    INSERT INTO WIND_TURBINE (
        idtemp, 
        powerfactor,
        hydraulicpressure,
        temperature,
        timestamp    
    ) VALUES (%s, %s, %s, %s, %s);
    """,
    task_group= group_postgres,
    dag = dag
)

def check_temperature_task(ti):
    temperature = float(ti.xcom_pull(task_ids="load_data", key="temperature"))

    if temperature >= 24:
        return 'group_check_temp.send_alert_email'
    else:
        return 'group_check_temp.send_normal_email'

check_temperature_task =  BranchPythonOperator(
    task_id="check_temperature",
    python_callable=check_temperature_task,
    task_group= group_check_temp,
    provide_context = True,
    dag = dag
)

send_alert_email_task = EmailOperator(
    task_id="send_alert_email",
    to=['mmartinsanajulia@gmail.com'],
    subject= "Wind Turbine - Airflow alert!",
    html_content="""
        <h2>Alerta de Temperatura</h2>
        <p>DAG: wind_turbine_dag</p>
    """,
    task_group= group_check_temp,
    dag = dag
)

send_normal_email_task = EmailOperator(
    task_id="send_normal_email",
    to=['mmartinsanajulia@gmail.com'],
    subject= "Wind Turbine - Airflow notification!",
    html_content="""
        <h2>Temperatura Ok!</h2>
        <p>DAG: wind_turbine_dag</p>
    """,
    task_group= group_check_temp,
    dag = dag
)

# Flow
with group_check_temp:
    check_temperature_task >> [send_alert_email_task, send_normal_email_task]
    
with group_postgres:
    create_table_task >> insert_data_task

file_sensor_task >> load_data_task >> [group_check_temp, group_postgres ]