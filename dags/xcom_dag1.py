from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pendulum

def get_name():
    return "Yusuf"

def greet(age,ti):
    name= ti.xcom_pull(task_ids="xcom_task1")
    print(f"Hello world I am {name} {age}")

with DAG(
    dag_id='xcom_dag1',
    schedule=None,
    start_date=pendulum.datetime(2022,11,11, tz="UTC")
    ) as dag:
   
    xcom_task1 = PythonOperator(
        task_id='xcom_task1', 
        python_callable=get_name
        )

    xcom_task2 = PythonOperator(
        task_id='xcom_task2',
        python_callable=greet,
        op_kwargs={"age":24}
        )
    
    xcom_task1 >> xcom_task2