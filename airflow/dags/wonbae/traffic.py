from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests
import zipfile
import os

ZIP_URL = "https://www.its.go.kr/opendata/fileDownload/event/2025/20251114.zip"

local_zip_path = "/opt/airflow/data/event.zip"
extract_folder = "/opt/airflow/data/extracted/"


def download_zip():
    response = requests.get(ZIP_URL)

    os.makedirs("/opt/airflow/data", exist_ok=True)

    with open(local_zip_path, "wb") as f:
        f.write(response.content)


def unzip_and_upload_to_s3():
    with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_folder)

    s3 = S3Hook(aws_conn_id="aws-default")

    for file in os.listdir(extract_folder):
        if file.endswith(".csv"):
            local_file = os.path.join(extract_folder, file)

            # IAM 정책에 맞게 prefix 정리
            s3_key = f"rawdata/wonbae/{file}"

            s3.load_file(
                filename=local_file,
                bucket_name="traffic-s3-team4",  # 수정완료
                key=s3_key,
                replace=True,
            )


with DAG(
    dag_id="seoul_incident_zip_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["seoul", "incident", "zip"],
) as dag:

    t1 = PythonOperator(
        task_id="download_zip_file", python_callable=download_zip
    )

    t2 = PythonOperator(
        task_id="unzip_and_upload_s3", python_callable=unzip_and_upload_to_s3
    )

    t1 >> t2
