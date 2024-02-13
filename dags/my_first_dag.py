from datetime import datetime, timedelta

from airflow import DAG 

from airflow.operators.bash import BashOperator

default_args = {
    'owner':'LostAhmado',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='my_first_dag',
    default_args= default_args,
    description='This is not really my first DAG, lol',
    start_date=datetime(2024,2,6,2),
    schedule_interval='@daily'
) as dag:
    pass

    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = "echo hello world, this is my first task"
    )

    task2 = BashOperator(
        task_id = 'Second_task',
        bash_command='echo I will run after the first task, I am task2'
    )

    task3 = BashOperator(
        task_id = 'Third_task',
        bash_command='echo I will run after the second task, I am task3'
    )
    
    
    task1>>task2
    task1>>task3
    