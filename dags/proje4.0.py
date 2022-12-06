from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

import pandas as pd # read csv, df manipulation
import pandas_gbq
import time # to simulate a real time data, time loop 
import os
from google.cloud import bigquery
from google.cloud import storage
from io import StringIO
import csv


def read_cloud(bucket_name, blob_name,ti):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =  r'/opt/airflow/dags/astute.json'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.get_blob(blob_name)
    blob = blob.download_as_string()
    print(type(blob))
    s=str(blob,'utf-8')

    data = StringIO(s) 

    df=pd.read_csv(data)
    df=df.to_json(orient = "records")
    ti.xcom_push(key='data_frame', value=df)

def bigquery_insert_db(ti):
    data= ti.xcom_pull(task_ids="task1",key="data_frame")
    data = pd.read_json(data)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =  r'/opt/airflow/dags/astute.json'
    client = bigquery.Client()

    table_id = "idsa.deneme1"
    project_id="astute-fort-366318"
    pandas_gbq.to_gbq(data, table_id, project_id=project_id, if_exists='append')

with DAG(
    dag_id='proje11',schedule=None,start_date=pendulum.datetime(2022,12,5, tz="UTC")
    ) as dag:
   
    xcom_task3 = PythonOperator(
        task_id='task1', 
        python_callable=read_cloud,
        op_kwargs={
                "bucket_name":"idsa-proje",
                "blob_name":"deneme.csv"}
        )


    xcom_task4 = PythonOperator(
        task_id='task2',
        python_callable=bigquery_insert_db
        )


    xcom_task3 >> xcom_task4