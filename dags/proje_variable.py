from airflow.models import Variable
import re
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import yaml
import pendulum
import os
import glob
import json

CONFIG_PATH = "include/confs/dynamic_dag.yaml"


def _list_files(**kwargs):
    # Parametrelerden değer alıyoruz
    directory = kwargs.get('templates_dict').get('directory')
    extension = kwargs.get('params')['file_extension']

    #Listeliyoruz
    file_list = glob.glob(os.path.join(directory, f"*.{extension}"))  # ["include/files/2022/10/12/17/a.txt", b.txt ... ..]

    os.path.join(directory, f"*.{extension}") # "include/files/2022/10/12/17/*.txt"


    print(directory)
    print(extension)
    print(file_list)

    # Regex kullanıldı
    # Önünde ne olursa olsun sonu txt ile bitsin
    pattern = "\w+\.txt$"
    file_names = []

    #Verileri json yapma
    for file_path in file_list:
        file_name = re.findall(pattern, file_path)
        file_names.append(file_name[0])

    json_list = json.dumps({"file_names": file_names})

    Variable.set(key='file_names', value=json_list)

    return file_list


with DAG(
  dag_id='project_with_variable',
  schedule=None,
  start_date=pendulum.datetime(2022,10,9, tz="UTC")
):

    # Config dosyasını okuma
    with open(CONFIG_PATH, "r") as yaml_file:
        config = yaml.load(yaml_file, Loader=yaml.FullLoader)

    #Templating dag konusu dinamiklik olması için gerekli
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
        #Dosya yolunu veriyoruz
        templates_dict={'directory': f"{config['file_path']}/{year}/{month}/{day}/{hour}/"},
        # Parammetre veriyoruz
        params={'file_extension': file_extension}
    )

    send_email = EmptyOperator(
        task_id='send_email'
    )

    files = Variable.get("file_names", deserialize_json=True)
    for file in files['file_names']:
        process_file = BashOperator(
            task_id=f"process_file_{file}",
            bash_command="sleep 1"
        )

        #Dinamik oalrak üzerilmesi gereken yer process_file kısmı
        check_file >> list_files >> process_file >> send_email