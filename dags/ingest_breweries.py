from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from spark.ingestion import fetch_and_store_json

PROJECT_ID = 'breweries.org.br'
REGION = 'us-central1'
CLUSTER = 'pipeline-ingestion-brewery'
BUCKET = 'hdfs-datalake-brewery'

with DAG(
    dag_id='ingest_breweries',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    default_args={'retries': 5}
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_breweries',
        python_callable=fetch_and_store_json,
        op_kwargs={'date': '{{ ds }}'}
    )

    transform_task = DataprocSubmitJobOperator(
        task_id='transform_breweries',
        region=REGION,
        project_id=PROJECT_ID,
        job={
            "placement": {"cluster_name": CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{BUCKET}/spark/transform.py",
                "args": ["{{ ds }}"]
            }
        }
    )

    aggregate_task = DataprocSubmitJobOperator(
        task_id='aggregate_breweries',
        region=REGION,
        project_id=PROJECT_ID,
        job={
            "placement": {"cluster_name": CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{BUCKET}/spark/aggregate.py",
                "args": ["{{ ds }}"]
            }
        }
    )

    fetch_task >> transform_task >> aggregate_task
