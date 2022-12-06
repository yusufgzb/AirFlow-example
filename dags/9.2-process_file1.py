from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator 
from airflow.utils.log.logging_mixin import LoggingMixin
import os,yaml
import pendulum

#check_file>> list_file>> process_file
CONFİG_PATH = "include/confs/dynamic_dag.yaml"

def _get_file(ti):
    ti.xcom_push(key="filepath",value="include/files/flights.txt")
    print("push include/files/flights.txt")

def _process_file(ti):
    file_path=ti.xcom_pull(key="filepath",value="incl")
    print("push include/files/flights.txt")


with DAG(
    dag_id='process_file1',
    schedule=None,
    start_date=pendulum.datetime(2022,12,2,tz="utc")
) as dag:

    with open(CONFİG_PATH, "r") as yaml_file:
        config = yaml.load(yaml_file,Loader=yaml.FullLoader)

    year = '{{ execution_date.strftime(%Y)}}'
    mount = '{{ execution_date.strftime(%m)}}'
    day = '{{ execution_date.strftime(%d)}}'
    hour = '{{ execution_date.strftime(%H)}}'
    files = config['file_names']

    for file in files:

        check_file = FileSensor(
            task_id=f"check_file_{file}",
            fs_conn_id="filepath",
            filepath=f"{config['file_path']}/{year}/{mount}/{day}/{hour}/{file}",
            poke_interval=10,
            timeout=30
        )

        # get_file = PythonOperator(
        #     task_id=f"get_file_{file}",
        #     python_callable=_get_file
        # )

        # process_file = PythonOperator(
        #     task_id=f"process_file_{file}",
        #     python_callable=_get_file
        # )
        # check_file>> get_file
