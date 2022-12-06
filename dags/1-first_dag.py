from airflow import DAG
from airflow.operators.bash import BashOperator

import pendulum

with DAG(
    dag_id='first_dag',
    schedule=None,
    start_date=pendulum.datetime(2022,11,11, tz="UTC")
) as dag:

    task1= BashOperator(
        task_id='task1',
        bash_command='echo hello world'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='sleep 3'
    )

    task1 >> task2 #Önce task1 sonra task2 çalışacak