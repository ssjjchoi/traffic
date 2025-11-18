import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator


def delete_local():
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday_dir = Path("/opt/airflow/data") / yesterday
    if yesterday_dir.exists():
        os.remove(yesterday_dir)
    print(f"Data deleted from {yesterday_dir}")


with DAG(
    dag_id="delete_local_data",
    start_date=datetime(2025, 11, 17),
    catchup=False,
    tags=["data_pipeline"],
    schedule="5 12 * * *",
) as dag:

    delete_local_data = PythonOperator(
        task_id="delete_local",
        python_callable=delete_local,
        dag=dag,
    )

    delete_local_data
