from datetime import datetime,timedelta
from airflow.decorators import dag, task

import pendulum

@dag(dag_id='task_dag',
    schedule=None,
    start_date=pendulum.datetime(2022,11,11, tz="UTC")
    )

def hello_world_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name":"Yusuf",
            "last_name":"Gzb"
        }
    @task()
    def get_age():
        return 24

    @task()
    def greet(first_name,last_name,age):
        print(f"Hello world I am {first_name} {last_name} {age}")    
    
    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict["first_name"],last_name=name_dict["last_name"], age=age) 
greet_dag=hello_world_etl()