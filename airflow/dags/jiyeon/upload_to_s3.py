from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import json


def upload():
    hook = S3Hook(aws_conn_id="aws_s3")
    data = {"a": 1, "b": 2}

    hook.load_string(
        string_data=json.dumps(data),
        key="rawdata/jiyeon/event_data.csv",
        bucket_name="traffic-s3-team4",
        replace=True,
    )


dag = DAG(
    dag_id="upload_to_s3",
    start_date=datetime(2022, 5, 5),
    catchup=False,
    tags=["test"],
    schedule="0 2 * * *",
)

upload_json = PythonOperator(
    task_id="upload_json",
    python_callable=upload,
    dag=dag,
)
