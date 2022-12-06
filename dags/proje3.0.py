from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

import os,json,yaml
from datetime import datetime
import pendulum
CONFIG_PATH="include/confs/proje3.proje3.yaml"

#Veri Yazma


def _read_json():
    now = datetime.now()

    filename = f"include/db/{str(now.year)}/{str(now.month)}/{str(now.day)}/{str(now.hour)}/{str(now.minute)}"
    
    with open(f"{filename}/sample.json", "r") as openfile:
        json_file=json.load(openfile)
    value_json=json_file["dt"]
    
    
def _insert_db(ti):
    dt= ti.xcom_pull(task_ids="python_task2",key="dt")
    
    # task2 = PostgresOperator(
    #     task_id='insert_table',
    #     postgres_conn_id='postgres_localhost',
    #     sql="""
    #         insert into dag_runs ( dt, dag_id) values ('{{ dt }}', '{{ dag.dag_id }}') 
        
    #     """
    #         )

    print(dt)


with DAG(
    dag_id='proje3',
    schedule=None,
    start_date=pendulum.datetime(2022,12,3, tz="UTC")
) as dag:


    python_task1 = BashOperator(
        task_id='python_task1', 
        bash_command='python /opt/airflow/scripts/proje3.1_create_json.py'
        )
    
    postgres_task1 = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs(
                dt date
            )
        
            """
            )
    python_task2 = BashOperator(
        task_id='python_task2', 
        bash_command='python /opt/airflow/scripts/proje3.2_insert_postgresql.py'
        )



    python_task1>>postgres_task1>>python_task2

    # task1 = PostgresOperator(
    #     task_id='create_table',
    #     postgres_conn_id='postgres_localhost',
    #     sql="""
    #         create table if not exists dag_runs(
    #             dt date,
    #             dag_id character varying,
    #             primary key (dt, dag_id)
    #         )
        
    #     """
    #         )
    # task2 = PostgresOperator(
    #     task_id='insert_table',
    #     postgres_conn_id='postgres_localhost',
    #     sql="""
    #         insert into dag_runs ( dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}') 
        
    #     """
    #         )

    