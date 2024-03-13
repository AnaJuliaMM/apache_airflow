# Imports 
from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd 


# DAG object
dag = DAG('producer_dag', 
          description='DAG producer',
          schedule_interval= None,
          start_date=datetime(2024, 3, 12),
          catchup=False,
          tags=['dataset-scheduled'])

dataset = Dataset('/opt/airflow/data/Churn_new.csv')


def copy_dataset():
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    dataset.to_csv('/opt/airflow/data/Churn_new.csv',  sep=';')

task1 = PythonOperator(
    task_id="task1",
    python_callable=copy_dataset,
    dag=dag,
    outlets=[dataset]
)

task1