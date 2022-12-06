from airflow import DAG
from airflow.operators.bash import BashOperator

import pendulum

with DAG(
    dag_id='multiple_dependencies',
    schedule=None,
    start_date=pendulum.datetime(2022,11,12, tz="UTC")
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='sleep 5'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='sleep 5'
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='sleep 5'
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command='sleep 5'
    )

    task5 = BashOperator(
        task_id='task5',
        bash_command='sleep 5'
    )

    task1 >> task2
    task3 >> task4
    [task2, task4] >> task5