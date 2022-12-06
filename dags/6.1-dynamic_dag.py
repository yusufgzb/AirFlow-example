from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum


with DAG(
    dag_id='dynamic_dag',
    start_date=pendulum.datetime(2022,11,11, tz="UTC"),
    schedule_interval="0 3 * * Tue,Sat"
) as dag:
    files = ["a","b","c","d","e"]
    for file in files:

        extract = BashOperator(
            task_id=f'extract_{file}',
            bash_command='echo helloworld'
        )

        transform = BashOperator(
            task_id=f'transform_{file}',
            bash_command='echo helloworld1'
        )

        load = BashOperator(
            task_id=f'load_{file}',
            bash_command='echo helloworld3'
        )

        extract >> transform >> load

