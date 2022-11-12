from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pendulum

def my_func():
    print('Hello from my_func')

def my_func2():
    print('Hello from my_func2********')

def greet(age,ti):
    name= ti.xcom_pull(task_ids="xcom_task1")
    print(f"Hello world I am {name} {age}")

def get_name():
    return "Yusuf"


def get_name2(ti):
    ti.xcom_push(key='first_name', value="Yusuf")
    ti.xcom_push(key='last_name', value="Gzb")
def greet2(age,ti):
    first_name= ti.xcom_pull(task_ids="xcom_task3",key="first_name")
    last_name= ti.xcom_pull(task_ids="xcom_task3",key="first_name")

    print(f"Hello world I am {first_name} ,{last_name} {age}")


with DAG(
    dag_id='xcom_dag',
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
    
    xcom_task3 = PythonOperator(
        task_id='xcom_task3', 
        python_callable=get_name2
        )

    xcom_task4 = PythonOperator(
        task_id='xcom_task4',
        python_callable=greet2,
        op_kwargs={"age":24}
        )


    xcom_task1 >> xcom_task2 >> xcom_task3 >> xcom_task4
    
