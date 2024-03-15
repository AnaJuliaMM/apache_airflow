from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from big_data_operator import BigDataOperator

# DAG object
dag = DAG('bigdata_operator_dag', 
          description='My first plugin operator ',
          schedule_interval=None,
          start_date=datetime(2024, 3, 12),
          catchup=False)

# Tasks
big_data = BigDataOperator(
    task_id='big_data',
    file_path = '/opt/airflow/data/Churn.csv',
    file_type='json',
    destination_path = '/opt/airflow/data/Churn.json',
    dag=dag
)

big_data