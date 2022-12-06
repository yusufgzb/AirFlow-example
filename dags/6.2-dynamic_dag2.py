from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum
import yaml

CONFİG_PATH = "include/confs/dynemic_dag.yaml"
with DAG(
    dag_id='dynamic_dag2',
    start_date=pendulum.datetime(2022,11,11, tz="UTC"),
    schedule_interval="0 3 * * Tue,Sat"
) as dag:

    with open(CONFİG_PATH, "r") as yaml_file:
        config = yaml.load(yaml_file,Loader=yaml.FullLoader)

    files = config["file_names"]
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

