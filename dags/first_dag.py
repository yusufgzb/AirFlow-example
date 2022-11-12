from airflow import DAG
from airflow.operators.bash import BashOperator

import pendulum

with DAG(
    dag_id='first_dag',
    schedule=None,
    start_date=pendulum.datetime(2022,11,11, tz="UTC")
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command='echo helloworld'
    )

    transform = BashOperator(
        task_id='transform',
        bash_command='sleep 3'
    )

    load = BashOperator(
        task_id='load',
        bash_command='sleep 10'
    )

    extract >> transform >> load

