from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.contrib.operators.dataproc_operator import DataprocCreateClusterOperator,DataProcPySparkOperator,DataprocDeleteClusterOperator
from airflow.operators.bash import BashOperator

PROJECT_ID=""

CLUSTER_NAME = "dataproc"

REGION = "us-central1"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    }
    ,
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
}

# Airflow DAG objesi oluşturuyoruz
with DAG(
    "aa_spark_bigquery",  # DAG'ın adı
    schedule_interval="@hourly",  # DAG'ın zaman aralığı
    catchup=False,  # Eksik günleri yakalama özelliği
    start_date=days_ago(1),  # Başlangıç tarihi
) as dag:
   
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        )


    pyspark_job = DataProcPySparkOperator(
            task_id='pyspark_shakespeare_task',
            main='gs://bucket_id/file_name.py',
            cluster_name=CLUSTER_NAME,
            region=REGION,
            job_name='word-count',
            dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar']
            )
    delete_cluster = DataprocDeleteClusterOperator(
       task_id="delete_cluster",
       project_id=PROJECT_ID,
       cluster_name=CLUSTER_NAME,
       region=REGION,
        ) 
    
    create_cluster >> pyspark_job >> delete_cluster
    








