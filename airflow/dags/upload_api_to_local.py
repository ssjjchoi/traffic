import requests
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable

from airflow import DAG

from datetime import datetime
import pandas as pd
from pathlib import Path


def upload_api_to_local(**context) -> str:
    API_KEY = Variable.get("ITS_API_KEY")
    url = f"https://openapi.its.go.kr:9443/eventInfo?apiKey={API_KEY}&type=all&eventType=all&getType=json"
    response = requests.get(url)
    data = response.json()

    # 오늘 날짜로 폴더 경로 생성
    today = datetime.now().strftime("%Y-%m-%d")
    data_dir = Path("/opt/airflow/data") / today
    data_dir.mkdir(parents=True, exist_ok=True)

    # 데이터를 DataFrame으로 변환
    try:
        items = data["body"]["items"]
        df = pd.DataFrame(items)
    except Exception as e:
        print(f"Error: {e}")
        df = pd.json_normalize(data)
        print(f"Error: {e}")
        return None

    # csv 파일로 저장
    datetime_str = datetime.now().strftime("%H:%M:%S")
    csv_path = data_dir / f"[traffic_event]{datetime_str}.csv"
    df.to_csv(csv_path, index=False)

    print(f"Data saved to {csv_path}")
    print(f"Total records: {len(df)}")

    # S3 key 생성
    s3_key = f"rawdata/jiyeon/event_data/{today}/{csv_path.name}"
    context["ti"].xcom_push(key="s3_key", value=s3_key)
    context["ti"].xcom_push(key="file_name", value=csv_path.name)

    return str(csv_path)


def upload_csv_to_s3(**context) -> str:
    ti = context["ti"]
    csv_path = ti.xcom_pull(task_ids="upload_api_to_local", key="return_value")
    s3_key = ti.xcom_pull(task_ids="upload_api_to_local", key="s3_key")

    if not csv_path or not s3_key:
        raise ValueError("CSV path or S3 key not found from previous task")

    s3_hook = S3Hook(aws_conn_id="aws_s3")
    csv_path = Path(csv_path)

    s3_hook.load_file(
        filename=str(csv_path),
        key=s3_key,
        bucket_name="traffic-s3-team4",
        replace=True,
    )
    print(f"Data uploaded to S3: s3://traffic-s3-team4/{s3_key}")

    # 다음 task에서 사용할 수 있도록 전달
    ti.xcom_push(key="s3_key", value=s3_key)
    ti.xcom_push(
        key="file_name",
        value=ti.xcom_pull(task_ids="upload_api_to_local", key="file_name"),
    )

    return s3_key


with DAG(
    dag_id="upload_api_to_local_and_s3",
    start_date=datetime(2022, 5, 5),
    catchup=False,
    tags=["data_pipeline"],
    schedule="0 */3 * * *",
) as dag:

    upload_api_to_local_task = PythonOperator(
        task_id="upload_api_to_local",
        python_callable=upload_api_to_local,
    )

    upload_csv_to_s3_task = PythonOperator(
        task_id="upload_csv_to_s3",
        python_callable=upload_csv_to_s3,
    )

    # 임시 테이블에 데이터 로드 및 병합
    load_and_merge_table = SnowflakeOperator(
        task_id="load_and_merge_table",
        snowflake_conn_id="snowflake_conn",
        sql="""
            -- 임시 테이블 생성
            CREATE TEMPORARY TABLE IF NOT EXISTS traffic_event_temp_jiyeon
            LIKE traffic_event_jiyeon;
            
            -- S3에서 임시 테이블로 데이터 로드 (SELECT 서브쿼리 사용)
            COPY INTO traffic_event_temp_jiyeon
            FROM (
                SELECT 
                    $1::VARCHAR as type,
                    $2::VARCHAR as eventType,
                    $3::VARCHAR as eventDetailType,
                    TO_TIMESTAMP_NTZ($4, 'YYYYMMDDHH24MISS') as startDate,
                    CASE 
                        WHEN $5 = '' OR $5 = '-' OR $5 IS NULL THEN NULL
                        ELSE $5::FLOAT
                    END as coordX,
                    CASE 
                        WHEN $6 = '' OR $6 = '-' OR $6 IS NULL THEN NULL
                        ELSE $6::FLOAT
                    END as coordY,
                    CASE 
                        WHEN $7 = '' OR $7 = '-' OR $7 IS NULL THEN NULL
                        ELSE $7::BIGINT
                    END as linkId,
                    $8::VARCHAR as roadName,
                    CASE 
                        WHEN $9 = '' OR $9 = '-' OR $9 IS NULL THEN NULL
                        ELSE $9::BIGINT
                    END as roadNo,
                    $10::VARCHAR as roadDrcType,
                    $11::VARCHAR as lanesBlockType,
                    $12::VARCHAR as lanesBlocked,
                    $13::VARCHAR as message,
                    CASE 
                        WHEN $14 = '' OR $14 IS NULL THEN NULL
                        ELSE TO_TIMESTAMP_NTZ($14, 'YYYYMMDDHH24MISS')
                    END as endDate,
                    CURRENT_TIMESTAMP() as createdAt
                FROM @s3_traffic_stage/{{ ti.xcom_pull(task_ids="upload_csv_to_s3", key="s3_key") }}
            )
            FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
            
            -- 기존 테이블 데이터를 임시 테이블에 복사
            INSERT INTO traffic_event_temp_jiyeon
            SELECT * FROM traffic_event_jiyeon;
            
            -- 기존 데이터 삭제
            DELETE FROM traffic_event_jiyeon;
        
            -- 중복 제거하여 최종 테이블에 삽입
            INSERT INTO traffic_event_jiyeon (
                type, eventType, eventDetailType, startDate, coordX, coordY,
                linkId, roadName, roadNo, roadDrcType, lanesBlockType,
                lanesBlocked, message, endDate, createdAt
            )
            SELECT 
                type, eventType, eventDetailType, startDate, coordX, coordY,
                linkId, roadName, roadNo, roadDrcType, lanesBlockType,
                lanesBlocked, message, endDate, createdAt
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY startDate, coordX, coordY
                           ORDER BY createdAt DESC
                       ) AS rn
                FROM traffic_event_temp_jiyeon
            ) 
            WHERE rn = 1;
            
            -- 임시 테이블 삭제
            DROP TABLE IF EXISTS traffic_event_temp_jiyeon;
        """,
    )

    (upload_api_to_local_task >> upload_csv_to_s3_task >> load_and_merge_table)
