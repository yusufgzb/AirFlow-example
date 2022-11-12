from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pendulum

def my_func():
    print('Hello from my_func')

def my_func2():
    print('Hello from my_func2********')

def greet(name,age):
    print(f"Hello world I am {name} {age}")

with DAG(
    dag_id='python_dag',
    schedule=None,
    start_date=pendulum.datetime(2022,11,11, tz="UTC")
    ) as dag:
   
    python_task1 = PythonOperator(
        task_id='python_task1',
        python_callable=my_func
        )
    python_task2 = PythonOperator(
        task_id='python_task2', 
        python_callable=my_func2
        )
    python_task3 = PythonOperator(
        task_id='python_task3', 
        python_callable=greet,
        op_kwargs={"name":"Yusuf","age":24}
        )

    python_task1 >> python_task2 >> python_task3