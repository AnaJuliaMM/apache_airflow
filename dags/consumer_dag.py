# Imports 
from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd 

# Dataset
dataset = Dataset('/opt/airflow/data/Churn_new.csv')

# DAG object
dag = DAG('consumer_dag', 
          description='DAG consumer',
          schedule= [dataset],
          start_date=datetime(2024, 3, 12),
          catchup=False,
          tags=['dataset-scheduled'])

def copy_dataset():
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    dataset.to_csv('/opt/airflow/data/Churn_new2.csv',  sep=';')

# Tasks
task1 = PythonOperator(
    task_id="task1",
    python_callable=copy_dataset,
    dag=dag,
    provide_context=True
)

# Flow
task1