# Imports 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd 
import statistics as sts

# DAG object
dag = DAG('data_cleaning_dag', 
          description='Python Operator example with cleaning data process',
          schedule_interval= None,
          start_date=datetime(2024, 3, 12),
          catchup=False)


# Functinos

def data_cleaner():
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=';')
    # Refactor columns name
    dataset.columns = ['Id', 'Score', 'State', 'Genre', 'Age', 'Assets', 'Account Balance', 'Products', 'HasCreditCard', 'IsActive', 'Salary', 'Exited' ]

    # Fill empty salary field with salary median
    salary_median = sts.median(dataset['Salary'])
    dataset['Salary'].fillna(salary_median, inplace=True)

    # Fill empty genre field with male
    dataset['Genre'].fillna('Masculino', inplace=True)

    # Select unreal age range and fill with age median 
    age_median = sts.median(dataset['Age']) 
    dataset.loc[(dataset['Age']<0) | (dataset['Age']>120), 'Age'] = age_median

    # Remove duplicate id
    dataset.drop_duplicates(subset='Id', keep='first', inplace=True)

    # Create a clean csv file
    dataset.to_csv("/opt/airflow/data/Churn_clean.csv", sep=';', index=False)


clean_data = PythonOperator(
    task_id="clean_data",
    python_callable= data_cleaner,
    dag=dag)
