import os,json,yaml
from datetime import datetime


def _create_json():
    now = datetime.now()
    json_file= {
        "dt":str(now.year)+"-"+str(now.month)+"-"+str(now.day)
    }
    json_object = json.dumps(json_file, indent=4)
    filename = f"/opt/airflow/include/db/{str(now.year)}/{str(now.month)}/{str(now.day)}/{str(now.minute)}"
    if not os.path.exists(filename):
        os.makedirs(filename)
    
    with open(f"{filename}/sample.json", "w") as outfile:
        outfile.write(json_object)
        print(outfile)

if __name__ == "__main__":
    _create_json()