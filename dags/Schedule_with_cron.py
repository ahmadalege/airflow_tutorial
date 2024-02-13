from datetime import datetime, timedelta

from airflow import DAG 
from airflow.operators.bash import BashOperator

default_args={
    'owmer':'Ahmado',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_cron_scheduler',
    default_args=default_args,
    start_date=datetime(2024,2,6),
    schedule_interval='0 0 * * *' #daily, '0 3 * * Tue,Fri'(every Tuesday and Friday at 3am)
) as dag:
    
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo dag with cron scheduler'
    )
    
    task1
    