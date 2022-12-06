import os,json,yaml
from datetime import datetime
import psycopg2 



def _read_json():
    database_name="test"
    user_name="airflow"
    password="airflow"
    host_ip="localhost"
    host_port="5432"

    baglanti=psycopg2.connect(
            database=database_name,
            user=user_name,
            password=password,
            host=host_ip,
            port=host_port)

    baglanti.autocommit = True
    cursor = baglanti.cursor()

    now = datetime.now()

    filename = f"/opt/airflow/include/db/{str(now.year)}/{str(now.month)}/{str(now.day)}/{str(now.minute)}"
    
    with open(f"{filename}/sample.json", "r") as openfile:
        json_file=json.load(openfile)
    value_json=json_file["dt"]
    


    cursor.execute(f"INSERT into dag_runs(dt) values ('{value_json}')")
    baglanti.commit()
    baglanti.close()

    

if __name__ == "__main__":
    _read_json()