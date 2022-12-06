from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pendulum

with DAG(
    dag_id='postgres_dag',
    schedule=None,
    start_date=pendulum.datetime(2022,11,12, tz="UTC")
) as dag:

    task1 = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        
        """
            )
    task2 = PostgresOperator(
        task_id='insert_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into dag_runs ( dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}') 
        
        """
            )

    task1 >> task2