from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator 
import os
import pendulum

#check_file>> list_file>> process_file

def _get_file(ti):
    ti.xcom_push(key="filepath",value="include/files/flights.txt")
    print("push include/files/flights.txt")

def _process_file(ti):
    file_path=ti.xcom_pull(key="filepath",default=None)
    print("push include/files/flights.txt")


with DAG(
    dag_id='process_file',
    schedule=None,
    start_date=pendulum.datetime(2022,12,2,tz="utc")
) as dag:
    check_file = FileSensor(
        task_id="check_file",
        fs_conn_id="filepath",
        filepath="include/files/flights.txt",
        poke_interval=10,
        timeout=30
    )

    get_file = PythonOperator(
        task_id="get_file",
        python_callable=_get_file
    )

    process_file = PythonOperator(
        task_id="process_file",
        python_callable=_get_file
    )
    check_file>> get_file
