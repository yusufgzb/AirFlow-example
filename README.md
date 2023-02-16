Airflow

Docker ile yükleme

docker-comğose indirme

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.4.0/docker-compose.yaml'

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.0.1/docker-compose.yaml'

Klasör ve dosya kurulumu

mkdir -p ./dags ./logs ./plugins

echo -e "AIRFLOW_UID=$(id -u)" > .env

docker kurulumları

docker-compose up airflow-init

docker-compose up -d

aşağıdaki adresten giriş yapabilriz

0.0.0.0:8080

GCP VM makineye Kurulum

Yeni VM makine oluştur(22.04 LTS Pro Server)

Install Docker

sudo apt-get update

sudo apt-get upgrade

sudo apt-get install docker.io

sudo apt install docker-compose


sudo apt-get update

sudo docker -v

sudo docker-compose -v


-----------

Docker kurulduktan sonra

git clone https://github.com/yusufgzb/AirFlow-example.git

cd AirFlow-example

echo -e "AIRFLOW_UID=$(id -u)" > .env

sudo docker-compose up airflow-init

sudo docker-compose up -d
