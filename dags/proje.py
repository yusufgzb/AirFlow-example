from airflow import DAG, XComArg
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import yaml
import pendulum
import os
import glob

CONFIG_PATH = "include/confs/dynamic_dag.yaml"


def _list_files(**kwargs):
    directory = kwargs.get('templates_dict').get('directory')
    extension = kwargs.get('params')['file_extension']

    file_list = [[file] for file in glob.glob(os.path.join(directory, f"*.{extension}"))]
    print(file_list)
    return file_list


def _process_file(file):
    with open(str(file), "r") as f:
        lines = f.readlines()
    return file


with DAG(
  dag_id='project_1',
  schedule=None,
  start_date=pendulum.datetime(2022, 12,2, tz="UTC")
):

    with open(CONFIG_PATH, "r") as yaml_file:
        config = yaml.load(yaml_file, Loader=yaml.FullLoader)

    year = '{{ execution_date.strftime("%Y") }}'
    month = '{{ execution_date.strftime("%m") }}'
    day = '{{ execution_date.strftime("%d") }}'
    hour = '{{ execution_date.strftime("%H") }}'

    file_extension = config['file_extension']

    check_file = FileSensor(
        task_id=f'check_file',
        fs_conn_id="filepath",
        filepath=f"{config['file_path']}/{year}/{month}/{day}/{hour}/*.{file_extension}",
        poke_interval=10,
        timeout=30
    )

    list_files = PythonOperator(
        task_id="list_files",
        python_callable=_list_files,
        templates_dict={'directory': f"{config['file_path']}/{year}/{month}/{day}/{hour}/"},
        params={'file_extension': file_extension}
    )

    process_file = PythonOperator.partial(
        task_id="process_file",
        python_callable=_process_file
    ).expand(
        op_args=XComArg(list_files)
    )

    send_email = EmptyOperator(
        task_id='send_email'
    )

    check_file >> list_files >> process_file >> send_email