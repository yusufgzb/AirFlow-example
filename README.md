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